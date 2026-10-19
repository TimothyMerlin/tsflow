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

#' @rdname build_dag
#' @export
dag_to_igraph <- function(dag,
                          nodes = NULL,
                          upstream_of = NULL,
                          downstream_of = NULL,
                          include_focus = TRUE) {
  if (!requireNamespace("igraph", quietly = TRUE)) {
    fail("package `igraph` is required for `dag_to_igraph()`")
  }

  if (inherits(dag, "ExpandedNodeDAG")) {
    return(igraph::graph_from_data_frame(
      d = dag$edges[, c("from", "to"), drop = FALSE],
      directed = TRUE,
      vertices = data.frame(
        name = dag$nodes,
        label = unname(dag$labels[dag$nodes]),
        stringsAsFactors = FALSE
      )
    ))
  }

  subgraph <- dag_subgraph(
    dag = dag,
    nodes = nodes,
    upstream_of = upstream_of,
    downstream_of = downstream_of,
    include_focus = include_focus
  )

  igraph::graph_from_data_frame(
    d = subgraph$edges,
    directed = TRUE,
    vertices = data.frame(
      name = subgraph$nodes,
      stringsAsFactors = FALSE
    )
  )
}

#' @rdname build_dag
#' @export
dag_to_visnetwork <- function(dag,
                              nodes = NULL,
                              upstream_of = NULL,
                              downstream_of = NULL,
                              include_focus = TRUE,
                              width = "100%",
                              height = "480px",
                              physics = FALSE,
                              edge_labels = FALSE) {
  dag <- as_visnetwork_dag(dag)
  expanded_labels <- if (inherits(dag, "ExpandedNodeDAG")) dag$labels else NULL

  if (!requireNamespace("visNetwork", quietly = TRUE)) {
    fail("package `visNetwork` is required for `dag_to_visnetwork()`")
  }
  if (!requireNamespace("igraph", quietly = TRUE)) {
    fail("package `igraph` is required for `dag_to_visnetwork()`")
  }
  if (!requireNamespace("htmlwidgets", quietly = TRUE)) {
    fail("package `htmlwidgets` is required for `dag_to_visnetwork()`")
  }
  if (!requireNamespace("htmltools", quietly = TRUE)) {
    fail("package `htmltools` is required for `dag_to_visnetwork()`")
  }
  if (!is_scalar_string(width)) {
    fail("`width` must be a single string")
  }
  if (!is_scalar_string(height)) {
    fail("`height` must be a single string")
  }
  if (!is_scalar_flag(physics)) {
    fail("`physics` must be TRUE or FALSE")
  }
  if (!is_scalar_flag(edge_labels)) {
    fail("`edge_labels` must be TRUE or FALSE")
  }

  graph <- dag_to_igraph(
    dag = dag,
    nodes = nodes,
    upstream_of = upstream_of,
    downstream_of = downstream_of,
    include_focus = include_focus
  )

  layout <- igraph::layout_with_sugiyama(graph)$layout
  layout <- cbind(-layout[, 2], layout[, 1])
  edge_list <- igraph::as_edgelist(graph)
  edge_label_values <- igraph::E(graph)$label
  graph_nodes <- igraph::V(graph)$name
  graph_labels <- if (!is.null(expanded_labels)) {
    unname(expanded_labels[graph_nodes])
  } else if (!is.null(igraph::vertex_attr(graph, "label"))) {
    igraph::vertex_attr(graph, "label")
  } else {
    graph_nodes
  }

  node_status <- dag$node_status
  if (is.null(node_status)) {
    node_status <- stats::setNames(rep("ok", length(graph_nodes)), graph_nodes)
  } else {
    node_status <- node_status[graph_nodes]
    missing_status <- is.na(node_status)
    node_status[missing_status] <- "ok"
  }

  node_kind <- if (is.null(expanded_labels)) dag$node_kind else NULL
  if (is.null(node_kind)) {
    node_kind <- stats::setNames(rep("derived", length(graph_nodes)), graph_nodes)
  } else {
    node_kind <- node_kind[graph_nodes]
    missing_kind <- is.na(node_kind)
    node_kind[missing_kind] <- "derived"
  }

  node_role <- if (is.null(expanded_labels)) dag$node_role else NULL
  if (is.null(node_role)) {
    node_role <- stats::setNames(rep("derived", length(graph_nodes)), graph_nodes)
  } else {
    node_role <- node_role[graph_nodes]
    missing_role <- is.na(node_role)
    node_role[missing_role] <- "derived"
  }

  node_pipeline <- if (is.null(expanded_labels)) dag$node_pipeline else NULL
  if (is.null(node_pipeline)) {
    if (!is.null(expanded_labels)) {
      node_pipeline <- stats::setNames(
        lapply(graph_nodes, function(name) {
          list(input = name, steps = character())
        }),
        graph_nodes
      )
    } else {
      node_pipeline <- stats::setNames(
        replicate(length(graph_nodes), list(list(input = "<unknown>", steps = character())), simplify = FALSE),
        graph_nodes
      )
    }
  } else {
    node_pipeline <- node_pipeline[graph_nodes]
    for (idx in seq_along(node_pipeline)) {
      if (is.null(node_pipeline[[idx]])) {
        node_pipeline[[idx]] <- list(input = "<unknown>", steps = character())
      }
    }
    names(node_pipeline) <- graph_nodes
  }

  node_background <- function(status, role) {
    if (identical(status, "ok")) {
      return(switch(
        role,
        raw = "#E8F5E9",
        alias = "#FEF3C7",
        "#DCEBFA"
      ))
    }

    switch(
      status,
      cycle = "#FEF3C7",
      failed = "#7F1D1D",
      skipped = "#FCA5A5",
      "#DCEBFA"
    )
  }

  node_border <- function(status, role) {
    if (identical(status, "ok")) {
      return(switch(
        role,
        raw = "#2E7D32",
        alias = "#D97706",
        "#2B7CE9"
      ))
    }

    switch(
      status,
      cycle = "#D97706",
      failed = "#450A0A",
      skipped = "#EF4444",
      "#2B7CE9"
    )
  }

  node_font <- function(status) {
    switch(
      status,
      cycle = "#111827",
      failed = "#F9FAFB",
      skipped = "#111827",
      "#111827"
    )
  }

  node_shape <- function(role) {
    switch(
      role,
      raw = "ellipse",
      alias = "diamond",
      "box"
    )
  }

  node_title <- function(name, role, kind, pipeline) {
    if (!is.null(expanded_labels)) {
      return(sprintf(
        "<b>%s</b><br>%s",
        htmltools::htmlEscape(name),
        htmltools::htmlEscape(expanded_labels[[name]] %||% name)
      ))
    }

    role_label <- switch(
      role,
      raw = "Raw input",
      alias = "Derived alias",
      "Derived transform"
    )
    status_label <- switch(
      node_status[[name]],
      cycle = "Cycle detected",
      failed = "Failed",
      skipped = "Skipped",
      "Ready"
    )
    steps <- pipeline$steps
    if (identical(role, "raw")) {
      steps_html <- "raw input"
    } else if (identical(role, "alias") ||
               (length(steps) == 1L && identical(steps[[1L]], "pass-through"))) {
      steps_html <- "alias/pass-through"
    } else if (length(steps) == 0L) {
      steps_html <- "no additional transforms"
    } else {
      steps_html <- paste(
        sprintf("%d. %s", seq_along(steps), htmltools::htmlEscape(steps)),
        collapse = "<br>"
      )
    }

    sprintf(
      "<b>%s</b><br>%s (%s)<br>status: %s<br>input: %s<br>%s",
      htmltools::htmlEscape(name),
      htmltools::htmlEscape(role_label),
      htmltools::htmlEscape(kind),
      htmltools::htmlEscape(status_label),
      htmltools::htmlEscape(pipeline$input),
      steps_html
    )
  }

  nodes_df <- data.frame(
    id = graph_nodes,
    label = graph_labels,
    x = layout[, 1] * 200,
    y = layout[, 2] * 120,
    title = mapply(
      node_title,
      name = graph_nodes,
      role = unname(node_role[graph_nodes]),
      kind = unname(node_kind[graph_nodes]),
      pipeline = unname(node_pipeline[graph_nodes]),
      USE.NAMES = FALSE
    ),
    shape = vapply(node_role[graph_nodes], node_shape, character(1)),
    status = unname(node_status[graph_nodes]),
    color.background = mapply(
      node_background,
      status = unname(node_status[graph_nodes]),
      role = unname(node_role[graph_nodes]),
      USE.NAMES = FALSE
    ),
    color.border = mapply(
      node_border,
      status = unname(node_status[graph_nodes]),
      role = unname(node_role[graph_nodes]),
      USE.NAMES = FALSE
    ),
    color.highlight.background = mapply(
      node_background,
      status = unname(node_status[graph_nodes]),
      role = unname(node_role[graph_nodes]),
      USE.NAMES = FALSE
    ),
    color.highlight.border = mapply(
      node_border,
      status = unname(node_status[graph_nodes]),
      role = unname(node_role[graph_nodes]),
      USE.NAMES = FALSE
    ),
    font.color = vapply(node_status[graph_nodes], node_font, character(1)),
    stringsAsFactors = FALSE
  )

  edges_df <- data.frame(
    from = edge_list[, 1],
    to = edge_list[, 2],
    label = if (isTRUE(edge_labels) && !is.null(edge_label_values)) edge_label_values else rep("", nrow(edge_list)),
    arrows = "to",
    stringsAsFactors = FALSE
  )

  details_id <- paste0("tsflow-dag-details-", paste(sample(c(letters, 0:9), 10, replace = TRUE), collapse = ""))
  select_js <- sprintf(
    "function(params) {
       var network = this;
       var selected = params.nodes[0];
       var allNodes = network.body.data.nodes.get();
       var allEdges = network.body.data.edges.get();
       var keep = {};
       var queue = [selected];
       keep[selected] = true;

       while (queue.length > 0) {
         var current = queue.shift();
         allEdges.forEach(function(edge) {
           if (edge.to === current && !keep[edge.from]) {
             keep[edge.from] = true;
             queue.push(edge.from);
           }
         });
       }

       var detail = document.getElementById('%s');
       if (detail) {
         var path = allNodes
           .map(function(node) { return node.id; })
           .filter(function(id) { return !!keep[id]; });
         detail.innerHTML =
           '<strong>Selected node:</strong> ' + selected +
           '<br><strong>Upstream path:</strong> ' + path.join(' -&gt; ');
       }

       network.body.data.nodes.update(allNodes.map(function(node) {
         var active = !!keep[node.id];
         var baseBackground = node.color && node.color.background ? node.color.background : '#DCEBFA';
         var baseBorder = node.color && node.color.border ? node.color.border : '#2B7CE9';
         var baseFont = node.font && node.font.color ? node.font.color : '#111827';
         return {
           id: node.id,
           color: active
             ? { background: baseBackground, border: baseBorder, highlight: { background: baseBackground, border: baseBorder } }
             : { background: '#E5E7EB', border: '#C7CDD4', highlight: { background: '#E5E7EB', border: '#C7CDD4' } },
           font: { color: active ? baseFont : '#9CA3AF' }
         };
       }));

       network.body.data.edges.update(allEdges.map(function(edge) {
         var active = !!keep[edge.from] && !!keep[edge.to];
         return {
           id: edge.id,
           color: active
             ? { color: '#97C2FC', highlight: '#2B7CE9', hover: '#2B7CE9' }
             : { color: '#D1D5DB', highlight: '#D1D5DB', hover: '#D1D5DB' },
           font: { color: active ? '#374151' : '#9CA3AF' }
         };
       }));
     }",
    details_id
  )
  deselect_js <- sprintf(
    "function() {
       var network = this;
       var allNodes = network.body.data.nodes.get();
       var allEdges = network.body.data.edges.get();
       var detail = document.getElementById('%s');

       if (detail) {
         detail.innerHTML = 'Click a node to inspect the upstream calculation path.';
       }

       network.body.data.nodes.update(allNodes.map(function(node) {
         return {
           id: node.id,
           color: node.color,
           font: node.font
         };
       }));

       network.body.data.edges.update(allEdges.map(function(edge) {
         return {
           id: edge.id,
           color: { color: '#97C2FC', highlight: '#2B7CE9', hover: '#2B7CE9' },
           font: { color: '#374151' }
         };
       }));
     }",
    details_id
  )

  widget <- visNetwork::visNetwork(nodes_df, edges_df, width = width, height = height) |>
    visNetwork::visNodes() |>
    visNetwork::visEdges(smooth = FALSE, font = list(align = "middle")) |>
    visNetwork::visPhysics(enabled = physics) |>
    visNetwork::visInteraction(dragNodes = TRUE, dragView = TRUE, zoomView = TRUE) |>
    visNetwork::visEvents(
      selectNode = htmlwidgets::JS(select_js),
      deselectNode = htmlwidgets::JS(deselect_js)
    )

  htmlwidgets::prependContent(
    widget,
    htmltools::tags$div(
      id = details_id,
      style = paste(
        "margin: 0 0 12px 0;",
        "padding: 10px 12px;",
        "border: 1px solid #D1D5DB;",
        "border-radius: 6px;",
        "background: #F9FAFB;",
        "font-size: 0.95rem;"
      ),
      "Click a node to inspect the upstream calculation path."
    )
  )
}

as_visnetwork_dag <- function(x) {
  if (inherits(x, "ExpandedNodeDAG")) {
    return(x)
  }
  if (inherits(x, "PipelinePlan")) {
    return(x$dag)
  }
  if (inherits(x, "PipelineResult")) {
    return(x$dag)
  }
  if (inherits(x, "ExpandedNodeResult")) {
    return(x$dag)
  }

  assert_inherits(x, "DAGPlan", "dag")
  x
}

#' @rdname build_dag
#' @export
dag_to_dot <- function(dag,
                       nodes = NULL,
                       upstream_of = NULL,
                       downstream_of = NULL,
                       include_focus = TRUE,
                       graph_name = "tsflow",
                       rankdir = "LR") {
  subgraph <- dag_subgraph(
    dag = dag,
    nodes = nodes,
    upstream_of = upstream_of,
    downstream_of = downstream_of,
    include_focus = include_focus
  )

  if (!is_scalar_string(graph_name)) {
    fail("`graph_name` must be a single string")
  }
  if (!is_scalar_string(rankdir)) {
    fail("`rankdir` must be a single string")
  }

  lines <- c(
    sprintf("digraph %s {", dot_quote_id(graph_name)),
    sprintf("  rankdir=%s;", rankdir)
  )

  for (node in subgraph$nodes) {
    lines <- c(lines, sprintf("  %s;", dot_quote_id(node)))
  }

  if (nrow(subgraph$edges) > 0L) {
    for (idx in seq_len(nrow(subgraph$edges))) {
      lines <- c(
        lines,
        sprintf(
          "  %s -> %s;",
          dot_quote_id(subgraph$edges$from[[idx]]),
          dot_quote_id(subgraph$edges$to[[idx]])
        )
      )
    }
  }

  paste(c(lines, "}"), collapse = "\n")
}

#' @rdname build_dag
#' @export
write_dag_dot <- function(dag,
                          path,
                          nodes = NULL,
                          upstream_of = NULL,
                          downstream_of = NULL,
                          include_focus = TRUE,
                          graph_name = "tsflow",
                          rankdir = "LR") {
  if (!is_scalar_string(path)) {
    fail("`path` must be a single string")
  }

  dot <- dag_to_dot(
    dag = dag,
    nodes = nodes,
    upstream_of = upstream_of,
    downstream_of = downstream_of,
    include_focus = include_focus,
    graph_name = graph_name,
    rankdir = rankdir
  )

  writeLines(dot, con = path)
  invisible(path)
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

dot_quote_id <- function(x) {
  sprintf("\"%s\"", gsub("\"", "\\\\\"", x, fixed = TRUE))
}
