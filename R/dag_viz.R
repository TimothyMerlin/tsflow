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

dot_quote_id <- function(x) {
  sprintf("\"%s\"", gsub("\"", "\\\\\"", x, fixed = TRUE))
}
