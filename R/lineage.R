#' Lineage and Methodology Documentation
#'
#' `series_lineage()` collects what a pipeline says about one series: its
#' definition step by step, which input feeds which transform argument, the
#' shared parameters (`vars`) it uses, everything upstream down to the raw
#' sources, and the series that use it. `document_pipeline()` writes this as
#' Markdown, one page per series plus an index, with a Mermaid diagram of each
#' series' upstream lineage. The pages render on GitHub, in pkgdown and in
#' Quarto.
#'
#' @param plan A `PipelinePlan` from [plan_pipeline()], or a `PipelineResult`.
#'   Create the plan with the same `transforms` used to run the pipeline, so
#'   inputs can be labelled with transform argument names.
#' @param series Name of the series to describe.
#' @param transform_descriptions Optional named character vector describing
#'   transforms, e.g. `c(jsa = "Temporal disaggregation")`, shown next to each
#'   step that uses them. Names may also be the arithmetic steps `add`, `sub`,
#'   `mul` and `div`.
#' @param result Optional `PipelineResult` from running the pipeline. Adds each
#'   series' status, the period it covers and its latest value, and the hash
#'   of each raw series' data (see `result$input_hashes` in
#'   [evaluate_pipeline()]). Defaults to `plan` when `plan` is itself a result.
#' @param dir Directory to write the Markdown files to. Created if needed.
#'
#' @return `series_lineage()` returns a `SeriesLineage` list with elements
#'   `name`, `kind`, `description`, `metadata`, `source`, `input`, `steps`,
#'   `inputs`, `upstream`, `raw_sources`, `downstream`, `graph_edges` and
#'   `status`.
#'
#'   `document_pipeline()` writes `index.md` and `<series>.md` files and returns
#'   their paths invisibly.
#'
#' @name lineage
#' @export
series_lineage <- function(plan, series, transform_descriptions = NULL, result = NULL) {
  plan <- as_lineage_plan(plan)
  result <- lineage_result(plan, result)
  assert_transform_descriptions(transform_descriptions)
  assert_known_node(plan$dag, series)

  dag <- plan$dag
  spec <- series_spec_by_name(plan$pipeline, series)
  upstream <- upstream_nodes(dag, series)
  raw_upstream <- upstream[dag$node_kind[upstream] == "raw"]
  graph <- dag_subgraph(dag, upstream_of = series, include_focus = TRUE)

  lineage <- list(
    name = series,
    kind = spec$kind,
    description = spec$description,
    metadata = spec$metadata,
    source = if (identical(spec$kind, "raw")) spec$source else NULL,
    input = NULL,
    steps = empty_steps_table(),
    inputs = data.frame(series = character(), used_as = character(), stringsAsFactors = FALSE),
    upstream = upstream,
    raw_sources = raw_sources_table(plan$pipeline, raw_upstream, result),
    downstream = downstream_nodes(dag, series),
    graph_edges = graph$edges[, c("from", "to"), drop = FALSE],
    status = if (is.null(result)) NULL else series_status(result, series)
  )

  if (identical(spec$kind, "derived")) {
    decomposition <- decompose_node_expression(spec$expression)
    lineage$input <- decomposition$input
    lineage$steps <- steps_table(decomposition$steps, transform_descriptions)

    labels <- dag$edge_labels[[series]]
    lineage$inputs <- data.frame(
      series = names(labels),
      used_as = unname(labels),
      stringsAsFactors = FALSE
    )
  }

  structure(lineage, class = c("SeriesLineage", "tsflow_spec"))
}

#' @rdname lineage
#' @export
document_pipeline <- function(plan, dir, transform_descriptions = NULL, result = NULL) {
  plan <- as_lineage_plan(plan)
  result <- lineage_result(plan, result)
  if (!is_scalar_string(dir)) {
    fail("`dir` must be a single directory path")
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  lineages <- lapply(plan$dag$order, function(name) {
    series_lineage(plan, name, transform_descriptions = transform_descriptions, result = result)
  })

  paths <- vapply(lineages, function(lineage) {
    path <- file.path(dir, paste0(lineage$name, ".md"))
    writeLines(lineage_markdown(lineage), path, useBytes = TRUE)
    path
  }, character(1))

  index_path <- file.path(dir, "index.md")
  writeLines(lineage_index_markdown(plan, lineages), index_path, useBytes = TRUE)

  invisible(c(index_path, paths))
}

#' @export
print.SeriesLineage <- function(x, ...) {
  cat(lineage_markdown(x), sep = "\n")
  invisible(x)
}

as_lineage_plan <- function(plan) {
  if (inherits(plan, c("PipelinePlan", "PipelineResult"))) {
    return(plan)
  }
  fail("`plan` must be a `PipelinePlan` from `plan_pipeline()` or a `PipelineResult`")
}

lineage_result <- function(plan, result) {
  if (is.null(result) && inherits(plan, "PipelineResult")) {
    return(plan)
  }
  if (!is.null(result)) {
    assert_inherits(result, "PipelineResult", "result")
  }
  result
}

assert_transform_descriptions <- function(descriptions) {
  if (is.null(descriptions)) {
    return(invisible(NULL))
  }
  if (!is.character(descriptions) || is.null(names(descriptions)) || any(!nzchar(names(descriptions)))) {
    fail("`transform_descriptions` must be a named character vector")
  }
  invisible(descriptions)
}

series_spec_by_name <- function(pipeline, name) {
  pipeline$series[[match(name, vapply(pipeline$series, `[[`, character(1), "name"))]]
}

empty_steps_table <- function() {
  data.frame(
    step = integer(),
    transform = character(),
    text = character(),
    vars = character(),
    description = character(),
    stringsAsFactors = FALSE
  )
}

steps_table <- function(steps, transform_descriptions) {
  if (length(steps) == 0L) {
    return(empty_steps_table())
  }

  rows <- lapply(seq_along(steps), function(idx) {
    step <- steps[[idx]]
    node <- step$node
    if (identical(step$type, "call")) {
      transform <- node$name
      text <- format_call_node(node)
      used_vars <- collect_var_names(c(node$args, node$kwargs))
    } else {
      transform <- node$op
      text <- format_binary_op_node(node)
      used_vars <- collect_var_names(list(node$right))
    }

    description <- if (!is.null(transform_descriptions) && transform %in% names(transform_descriptions)) {
      transform_descriptions[[transform]]
    } else {
      NA_character_
    }

    data.frame(
      step = idx,
      transform = transform,
      text = text,
      vars = paste(unique(used_vars), collapse = ", "),
      description = description,
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

# Names of shared `vars` used anywhere in a list of expression nodes.
collect_var_names <- function(nodes) {
  unlist(lapply(nodes, function(node) {
    if (inherits(node, "LiteralNode")) {
      return(node$var)
    }
    if (inherits(node, "CallNode")) {
      return(collect_var_names(c(node$args, node$kwargs)))
    }
    if (inherits(node, "BinaryOpNode")) {
      return(collect_var_names(list(node$left, node$right)))
    }
    if (inherits(node, "PipeNode")) {
      return(collect_var_names(list(node$input, node$call)))
    }
    NULL
  }), use.names = FALSE) %||% character()
}

raw_sources_table <- function(pipeline, names, result = NULL) {
  rows <- lapply(names, function(name) {
    source <- series_spec_by_name(pipeline, name)$source
    data.frame(
      series = name,
      type = source$type,
      config = format_source_config(source$config),
      hash = input_hash(result, name),
      stringsAsFactors = FALSE
    )
  })
  if (length(rows) == 0L) {
    return(data.frame(
      series = character(),
      type = character(),
      config = character(),
      hash = character(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

input_hash <- function(result, name) {
  hashes <- result$input_hashes
  if (name %in% names(hashes)) hashes[[name]] else NA_character_
}

format_source_config <- function(config) {
  if (length(config) == 0L) {
    return("")
  }
  paste(
    sprintf("%s = %s", names(config), vapply(config, function(value) paste(format(value), collapse = ", "), character(1))),
    collapse = ", "
  )
}

series_status <- function(result, name) {
  status <- if (name %in% names(result$dag$node_status)) result$dag$node_status[[name]] else "not run"
  value <- result$values[[name]]

  period <- NA_character_
  latest <- NA_character_
  if (stats::is.ts(value)) {
    period <- format_ts_period(value)
  } else if (inherits(value, "xts")) {
    period <- format_xts_period(value)
  }
  if (!is.null(value) && length(value) > 0L) {
    latest <- format(as.numeric(utils::tail(value, 1L))[[1L]], digits = 6L)
  }

  list(status = status, period = period, latest_value = latest, input_hash = input_hash(result, name))
}

lineage_mermaid <- function(lineage) {
  nodes <- unique(c(lineage$graph_edges$from, lineage$graph_edges$to, lineage$name))
  ids <- stats::setNames(sprintf("n%d", seq_along(nodes)), nodes)

  c(
    "```mermaid",
    "flowchart LR",
    sprintf("  %s[\"%s\"]", ids, gsub("\"", "'", nodes, fixed = TRUE)),
    if (nrow(lineage$graph_edges) > 0L) {
      sprintf("  %s --> %s", ids[lineage$graph_edges$from], ids[lineage$graph_edges$to])
    },
    sprintf("  style %s stroke-width:3px", ids[[lineage$name]]),
    "```"
  )
}

# R code must be ASCII, so the middle dot and em dash are built from their
# code points.
md_dot_separator <- paste0(" ", intToUtf8(0xB7), " ")
md_dash_separator <- paste0(" ", intToUtf8(0x2014), " ")

md_link <- function(name) {
  sprintf("[%s](%s.md)", name, name)
}

md_cell <- function(x) {
  x[is.na(x)] <- ""
  gsub("|", "\\|", x, fixed = TRUE)
}

lineage_markdown <- function(lineage) {
  kind_label <- if (identical(lineage$kind, "raw")) "Raw series" else "Derived series"
  lines <- c(
    sprintf("# %s", lineage$name),
    "",
    paste(c(sprintf("*%s*", kind_label), lineage$description), collapse = md_dot_separator)
  )

  if (length(lineage$metadata) > 0L) {
    lines <- c(lines, "", sprintf("- **%s:** %s", names(lineage$metadata), vapply(lineage$metadata, function(v) paste(format(v), collapse = ", "), character(1))))
  }

  if (!is.null(lineage$status)) {
    status <- lineage$status
    lines <- c(lines, "", paste(c(
      sprintf("**Status:** %s", status$status),
      if (!is.na(status$period)) sprintf("**Period:** %s", status$period),
      if (!is.na(status$latest_value)) sprintf("**Latest value:** %s", status$latest_value),
      if (!is.na(status$input_hash)) sprintf("**Data hash:** `%s`", status$input_hash)
    ), collapse = md_dot_separator))
  }

  if (identical(lineage$kind, "raw")) {
    config <- format_source_config(lineage$source$config)
    lines <- c(lines, "", "## Source", "", sprintf("Type `%s`%s", lineage$source$type, if (nzchar(config)) sprintf(": %s", config) else ""))
  } else {
    step_lines <- vapply(seq_len(nrow(lineage$steps)), function(idx) {
      row <- lineage$steps[idx, ]
      extras <- c(
        if (nzchar(row$vars)) sprintf("uses var %s", paste(sprintf("`%s`", strsplit(row$vars, ", ", fixed = TRUE)[[1L]]), collapse = ", ")),
        if (!is.na(row$description)) row$description
      )
      sprintf("%d. `%s`%s", idx + 1L, row$text, if (length(extras) > 0L) paste0(md_dash_separator, paste(extras, collapse = "; ")) else "")
    }, character(1))

    lines <- c(
      lines,
      "",
      "## Definition",
      "",
      sprintf("1. Start from %s", md_link(lineage$input)),
      step_lines,
      "",
      "## Inputs",
      "",
      "| Series | Used as |",
      "|---|---|",
      sprintf("| %s | %s |", md_link(lineage$inputs$series), md_cell(lineage$inputs$used_as))
    )
  }

  lines <- c(lines, "", "## Lineage", "", lineage_mermaid(lineage))
  if (nrow(lineage$raw_sources) > 0L) {
    lines <- c(
      lines,
      "",
      "Raw sources:",
      "",
      sprintf(
        "- %s (`%s`%s)%s",
        md_link(lineage$raw_sources$series),
        lineage$raw_sources$type,
        ifelse(nzchar(lineage$raw_sources$config), paste0(": ", lineage$raw_sources$config), ""),
        ifelse(is.na(lineage$raw_sources$hash), "", sprintf("%sdata hash `%s`", md_dot_separator, lineage$raw_sources$hash))
      )
    )
  }

  lines <- c(
    lines,
    "",
    "## Used by",
    "",
    if (length(lineage$downstream) > 0L) sprintf("- %s", md_link(lineage$downstream)) else "Not used by other series."
  )

  lines
}

lineage_index_markdown <- function(plan, lineages) {
  with_status <- !is.null(lineages[[1L]]$status)
  header <- c("Series", "Kind", "Description", if (with_status) c("Status", "Period"))

  rows <- vapply(lineages, function(lineage) {
    cells <- c(
      md_link(lineage$name),
      lineage$kind,
      md_cell(lineage$description %||% ""),
      if (with_status) c(lineage$status$status, md_cell(lineage$status$period))
    )
    sprintf("| %s |", paste(cells, collapse = " | "))
  }, character(1))

  c(
    "# Pipeline lineage",
    "",
    sprintf("%d series. Each page shows how a series is defined and where its data comes from.", length(lineages)),
    "",
    sprintf("| %s |", paste(header, collapse = " | ")),
    sprintf("|%s", paste(rep("---|", length(header)), collapse = "")),
    rows
  )
}
