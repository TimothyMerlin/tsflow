#' Build and Query a Pipeline DAG
#'
#' Builds and queries a dependency graph for a normalized pipeline. Cycles raise
#' an error. Use `dag_to_igraph()` for lightweight graph inspection in R,
#' `dag_to_visnetwork()` for an interactive HTML widget, or `dag_to_dot()` when
#' you need a Graphviz export.
#'
#' @param pipeline A `PipelineSpec` object.
#' @param transforms A named list of functions used to resolve bare transform
#'   names while validating the pipeline and labelling edges. Namespaced
#'   transforms such as `stats::lag` do not need to be registered.
#' @param allow_cycles Whether to return a graph annotated with cycle nodes
#'   instead of failing when a dependency cycle is detected.
#' @param dag A `DAGPlan` object.
#' @param node A node name present in the DAG.
#' @param include_self Whether to include `node` in the returned result.
#' @param include_labels Whether to include edge labels in the `dag_edges()`
#'   result.
#' @param nodes Optional explicit node set to include in a subgraph or export.
#' @param upstream_of Optional focus nodes whose ancestors should be included.
#' @param downstream_of Optional focus nodes whose descendants should be
#'   included.
#' @param include_focus Whether focused nodes should be included in the result.
#' @param width Widget width passed to `visNetwork()`.
#' @param height Widget height passed to `visNetwork()`.
#' @param physics Whether visNetwork physics should be enabled.
#' @param edge_labels Whether to show edge labels in the visNetwork widget.
#' @param graph_name Graph name used in DOT output.
#' @param rankdir Graphviz rank direction such as `"LR"` or `"TB"`.
#' @param path Path to a `.dot` output file.
#'
#' @return
#' `build_dag()` returns a `DAGPlan` object containing nodes, dependencies,
#' downstream edges, and topological execution order.
#'
#' `upstream_nodes()` and `downstream_nodes()` return character vectors.
#'
#' `dag_edges()` returns a data frame with `from` and `to` columns, and
#' optionally a `label` column.
#'
#' `dag_subgraph()` returns a `DAGSubgraph` object containing `nodes` and
#' `edges`.
#'
#' `expand_node_dag()` returns a `DAGSubgraph`-like object containing the
#' internal step graph for one node.
#'
#' `dag_to_igraph()` returns an `igraph` object.
#'
#' `dag_to_visnetwork()` returns a `visNetwork` htmlwidget.
#'
#' `dag_to_dot()` returns a Graphviz DOT string.
#'
#' `write_dag_dot()` writes DOT text to disk and returns the path invisibly.
#'
#' @name build_dag
#' @importFrom stats setNames
#' @export
build_dag <- function(pipeline,
                      transforms = list(),
                      allow_cycles = FALSE) {
  validate_pipeline(pipeline, transforms = transforms)

  if (!is_scalar_flag(allow_cycles)) {
    fail("`allow_cycles` must be TRUE or FALSE")
  }

  series <- pipeline$series
  node_order <- vapply(series, function(item) item$name, character(1))
  dependencies <- stats::setNames(vector("list", length(series)), node_order)
  node_kind <- stats::setNames(vapply(series, `[[`, character(1), "kind"), node_order)
  node_role <- stats::setNames(rep("derived", length(series)), node_order)
  node_pipeline <- stats::setNames(vector("list", length(series)), node_order)

  for (idx in seq_along(series)) {
    item <- series[[idx]]
    if (identical(item$kind, "raw")) {
      dependencies[[item$name]] <- character()
      node_role[[item$name]] <- "raw"
      node_pipeline[[item$name]] <- list(
        input = item$name,
        steps = character()
      )
    } else {
      dependencies[[item$name]] <- unique(setdiff(
        extract_dependencies(item$expression),
        item$name
      ))
      node_role[[item$name]] <- if (inherits(item$expression, "RefNode")) "alias" else "derived"
      node_pipeline[[item$name]] <- summarize_pipeline_expr(item$expression)
    }
  }

  edge_labels <- stats::setNames(
    replicate(length(node_order), setNames(character(), character()), simplify = FALSE),
    node_order
  )

  for (idx in seq_along(series)) {
    item <- series[[idx]]
    if (identical(item$kind, "derived")) {
      edge_labels[[item$name]] <- describe_expression_edges(
        item$expression,
        transforms = transforms
      )
    }
  }

  downstream <- stats::setNames(
    replicate(length(node_order), character(), simplify = FALSE),
    node_order
  )

  indegree <- stats::setNames(integer(length(node_order)), node_order)

  for (name in node_order) {
    deps <- dependencies[[name]]
    indegree[[name]] <- length(deps)

    for (dep in deps) {
      downstream[[dep]] <- c(downstream[[dep]], name)
    }
  }

  queue <- node_order[indegree[node_order] == 0L]
  execution_order <- character()

  while (length(queue) > 0L) {
    current <- queue[[1L]]
    queue <- queue[-1L]
    execution_order <- c(execution_order, current)

    for (child in downstream[[current]]) {
      indegree[[child]] <- indegree[[child]] - 1L
      if (indegree[[child]] == 0L) {
        queue <- c(queue, child)
      }
    }
  }

  cycle_nodes <- character()
  if (length(execution_order) != length(node_order)) {
    cycle_nodes <- node_order[indegree[node_order] > 0L]
    if (!isTRUE(allow_cycles)) {
      fail(sprintf(
        "dependency graph contains a cycle involving: %s",
        paste(cycle_nodes, collapse = ", ")
      ))
    }
  }

  node_status <- stats::setNames(rep("ok", length(node_order)), node_order)
  if (length(cycle_nodes) > 0L) {
    node_status[cycle_nodes] <- "cycle"
  }

  structure(
    list(
      nodes = node_order,
      dependencies = dependencies,
      edge_labels = edge_labels,
      downstream = downstream,
      node_kind = node_kind,
      node_role = node_role,
      node_pipeline = node_pipeline,
      node_status = node_status,
      order = c(execution_order, node_order[!(node_order %in% execution_order)]),
      cycle_nodes = cycle_nodes,
      is_acyclic = length(cycle_nodes) == 0L
    ),
    class = c("DAGPlan", "tsflow_spec")
  )
}

summarize_pipeline_expr <- function(expr) {
  if (inherits(expr, "RefNode")) {
    return(list(
      input = expr$name,
      steps = "pass-through"
    ))
  }

  if (inherits(expr, "PipeNode")) {
    upstream <- summarize_pipeline_expr(expr$input)
    upstream_steps <- upstream$steps
    if (length(upstream_steps) == 1L && identical(upstream_steps[[1L]], "pass-through")) {
      upstream_steps <- character()
    }
    return(list(
      input = upstream$input,
      steps = c(upstream_steps, format_call_node(expr$call))
    ))
  }

  if (inherits(expr, "BinaryOpNode")) {
    upstream <- summarize_pipeline_expr(expr$left)
    upstream_steps <- upstream$steps
    if (length(upstream_steps) == 1L && identical(upstream_steps[[1L]], "pass-through")) {
      upstream_steps <- character()
    }
    return(list(
      input = upstream$input,
      steps = c(upstream_steps, format_binary_op_node(expr))
    ))
  }

  list(
    input = "<unknown>",
    steps = "<unknown>"
  )
}

format_call_node <- function(call) {
  args <- vapply(call$args, format_expr_arg, character(1))
  kwargs <- if (length(call$kwargs) > 0L) {
    paste0(names(call$kwargs), "=", vapply(call$kwargs, format_expr_arg, character(1)))
  } else {
    character()
  }

  pieces <- c(args, kwargs)
  sprintf("%s(%s)", call$name, paste(pieces, collapse = ", "))
}

format_binary_op_node <- function(node) {
  sprintf("%s(%s)", node$op, format_expr_arg(node$right))
}

format_expr_arg <- function(arg) {
  if (inherits(arg, "RefNode")) {
    return(arg$name)
  }

  if (inherits(arg, "LiteralNode")) {
    return(format_literal_value(arg$value))
  }

  if (inherits(arg, "CallNode")) {
    return(format_call_node(arg))
  }

  if (inherits(arg, "BinaryOpNode")) {
    return(format_binary_op_node(arg))
  }

  if (inherits(arg, "PipeNode")) {
    summary <- summarize_pipeline_expr(arg)
    return(paste(c(summary$input, summary$steps), collapse = " -> "))
  }

  "<expr>"
}

format_literal_value <- function(value) {
  if (is.null(value)) {
    return("NULL")
  }

  if (length(value) == 1L) {
    if (is.character(value)) {
      return(sprintf("'%s'", value))
    }
    return(as.character(value))
  }

  parts <- if (is.character(value)) {
    sprintf("'%s'", value)
  } else {
    as.character(value)
  }

  sprintf("c(%s)", paste(parts, collapse = ", "))
}

#' @rdname build_dag
#' @export
upstream_nodes <- function(dag, node, include_self = FALSE) {
  assert_inherits(dag, "DAGPlan", "dag")
  assert_known_node(dag, node)

  visited <- character()
  stack <- dag$dependencies[[node]]

  while (length(stack) > 0L) {
    current <- stack[[1L]]
    stack <- stack[-1L]

    if (current %in% visited) {
      next
    }

    visited <- c(visited, current)
    stack <- c(stack, dag$dependencies[[current]])
  }

  result <- order_nodes(dag, visited)
  if (include_self) {
    result <- order_nodes(dag, c(result, node))
  }

  result
}

#' @rdname build_dag
#' @export
downstream_nodes <- function(dag, node, include_self = FALSE) {
  assert_inherits(dag, "DAGPlan", "dag")
  assert_known_node(dag, node)

  visited <- character()
  stack <- dag$downstream[[node]]

  while (length(stack) > 0L) {
    current <- stack[[1L]]
    stack <- stack[-1L]

    if (current %in% visited) {
      next
    }

    visited <- c(visited, current)
    stack <- c(stack, dag$downstream[[current]])
  }

  result <- order_nodes(dag, visited)
  if (include_self) {
    result <- order_nodes(dag, c(node, result))
  }

  result
}

#' @rdname build_dag
#' @export
dag_edges <- function(dag, include_labels = FALSE) {
  assert_inherits(dag, "DAGPlan", "dag")

  if (!is_scalar_flag(include_labels)) {
    fail("`include_labels` must be TRUE or FALSE")
  }

  edge_from <- character()
  edge_to <- character()
  edge_label <- character()

  for (to in dag$nodes) {
    deps <- dag$dependencies[[to]]
    if (length(deps) == 0L) {
      next
    }

    edge_from <- c(edge_from, deps)
    edge_to <- c(edge_to, rep(to, length(deps)))

    if (include_labels) {
      labels <- dag$edge_labels[[to]]
      edge_label <- c(edge_label, unname(labels[deps]))
    }
  }

  if (include_labels) {
    return(data.frame(
      from = edge_from,
      to = edge_to,
      label = edge_label,
      stringsAsFactors = FALSE
    ))
  }

  data.frame(
    from = edge_from,
    to = edge_to,
    stringsAsFactors = FALSE
  )
}

#' @rdname build_dag
#' @export
dag_subgraph <- function(dag,
                         nodes = NULL,
                         upstream_of = NULL,
                         downstream_of = NULL,
                         include_focus = TRUE) {
  assert_inherits(dag, "DAGPlan", "dag")

  selected <- character()

  if (!is.null(nodes)) {
    if (!is.character(nodes)) {
      fail("`nodes` must be a character vector when provided")
    }
    for (node in nodes) {
      assert_known_node(dag, node)
    }
    selected <- c(selected, nodes)
  }

  if (!is.null(upstream_of)) {
    if (!is.character(upstream_of)) {
      fail("`upstream_of` must be a character vector when provided")
    }
    for (node in upstream_of) {
      selected <- c(selected, upstream_nodes(dag, node, include_self = include_focus))
    }
  }

  if (!is.null(downstream_of)) {
    if (!is.character(downstream_of)) {
      fail("`downstream_of` must be a character vector when provided")
    }
    for (node in downstream_of) {
      selected <- c(selected, downstream_nodes(dag, node, include_self = include_focus))
    }
  }

  if (length(selected) == 0L) {
    selected <- dag$nodes
  }

  selected <- order_nodes(dag, selected)
  edges <- dag_edges(dag, include_labels = TRUE)

  if (nrow(edges) > 0L) {
    keep <- edges$from %in% selected & edges$to %in% selected
    edges <- edges[keep, , drop = FALSE]
  }

  structure(
    list(
      nodes = selected,
      edges = edges
    ),
    class = c("DAGSubgraph", "tsflow_spec")
  )
}

#' @rdname build_dag
#' @export
expand_node_dag <- function(dag, node) {
  dag <- as_visnetwork_dag(dag)
  assert_known_node(dag, node)

  pipeline <- dag$node_pipeline[[node]]
  if (is.null(pipeline)) {
    fail(sprintf("node `%s` does not have pipeline metadata", node))
  }

  start_id <- pipeline$input
  steps <- pipeline$steps %||% character()
  if (length(steps) == 1L && identical(steps[[1L]], "pass-through")) {
    end_id <- node
    edges <- data.frame(
      from = start_id,
      to = end_id,
      label = "pass-through",
      stringsAsFactors = FALSE
    )
    nodes <- c(start_id, end_id)
  } else if (length(steps) == 0L) {
    end_id <- node
    edges <- data.frame(
      from = start_id,
      to = end_id,
      label = "input",
      stringsAsFactors = FALSE
    )
    nodes <- c(start_id, end_id)
  } else {
    step_ids <- sprintf("%s::step_%d", node, seq_along(steps))
    step_labels <- steps

    edge_from <- c(start_id, step_ids[-length(step_ids)])
    edge_to <- c(step_ids)
    edge_label <- c("input", rep("pipe", length(step_ids) - 1L))

    edges <- data.frame(
      from = c(edge_from, utils::tail(step_ids, 1L)),
      to = c(edge_to, node),
      label = c(edge_label, "output"),
      stringsAsFactors = FALSE
    )
    nodes <- c(start_id, step_ids, node)
  }

  structure(
    list(
      nodes = nodes,
      edges = edges,
      labels = stats::setNames(
        c(
          pipeline$input,
          if (exists("step_labels", inherits = FALSE)) step_labels else character(),
          node
        ),
        nodes
      ),
      focus = node
    ),
    class = c("ExpandedNodeDAG", "DAGSubgraph", "tsflow_spec")
  )
}

describe_expression_edges <- function(expr, transforms) {
  labels <- collect_edge_labels(expr, transforms = transforms)
  labels[!duplicated(names(labels))]
}

collect_edge_labels <- function(expr, context = NULL, transforms, positional_offset = 0L) {
  if (inherits(expr, "RefNode")) {
    return(stats::setNames(context %||% "input", expr$name))
  }

  if (inherits(expr, "LiteralNode")) {
    return(setNames(character(), character()))
  }

  if (inherits(expr, "CallNode")) {
    labels <- setNames(character(), character())
    formal_names <- resolve_transform_formals(expr$name, transforms = transforms)

    if (length(expr$args) > 0L) {
      for (idx in seq_along(expr$args)) {
        labels <- c(
          labels,
          collect_edge_labels(
            expr$args[[idx]],
            sprintf(
              "%s: %s",
              expr$name,
              formal_label_at(
                formal_names,
                idx + positional_offset,
                fallback = sprintf("arg %d", idx)
              )
            ),
            transforms = transforms,
            positional_offset = 0L
          )
        )
      }
    }

    if (length(expr$kwargs) > 0L) {
      for (key in names(expr$kwargs)) {
        labels <- c(
          labels,
          collect_edge_labels(
            expr$kwargs[[key]],
            sprintf("%s: %s", expr$name, key),
            transforms = transforms,
            positional_offset = 0L
          )
        )
      }
    }

    return(labels)
  }

  if (inherits(expr, "PipeNode")) {
    formal_names <- resolve_transform_formals(expr$call$name, transforms = transforms)
    return(c(
      collect_edge_labels(
        expr$input,
        sprintf("%s: %s", expr$call$name, formal_label_at(formal_names, 1L, fallback = "input")),
        transforms = transforms,
        positional_offset = 0L
      ),
      collect_edge_labels(expr$call, transforms = transforms, positional_offset = 1L)
    ))
  }

  if (inherits(expr, "BinaryOpNode")) {
    return(c(
      collect_edge_labels(expr$left, sprintf("%s: %s", expr$op, "left"), transforms = transforms, positional_offset = 0L),
      collect_edge_labels(expr$right, sprintf("%s: %s", expr$op, "right"), transforms = transforms, positional_offset = 0L)
    ))
  }

  fail(sprintf(
    "cannot describe edges for unsupported expression class `%s`",
    paste(class(expr), collapse = ", ")
  ))
}

resolve_transform_formals <- function(transform_name, transforms) {
  fn <- tryCatch(
    resolve_transform(transform_name, transforms = transforms),
    error = function(e) NULL
  )

  if (is.null(fn)) {
    return(character())
  }

  names(formals(fn)) %||% character()
}

formal_label_at <- function(formal_names, index, fallback) {
  if (length(formal_names) >= index &&
      !is.na(formal_names[[index]]) &&
      nzchar(formal_names[[index]]) &&
      !identical(formal_names[[index]], "...")) {
    return(formal_names[[index]])
  }

  fallback
}

assert_known_node <- function(dag, node) {
  if (!is_scalar_string(node) || !(node %in% dag$nodes)) {
    fail(sprintf("node `%s` is not present in the DAG", node))
  }
}

order_nodes <- function(dag, nodes) {
  dag$order[dag$order %in% unique(nodes)]
}
