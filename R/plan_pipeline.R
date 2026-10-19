#' Plan a Pipeline Without Executing It
#'
#' Reads, normalizes, validates, and builds a DAG plan for a pipeline without
#' executing any transforms.
#'
#' @param path Path to a pipeline definition file.
#' @param outputs Optional output names to plan for. When `NULL`, pipeline
#'   outputs are used if present, otherwise all nodes are included.
#' @param transforms A named list of functions used to resolve bare transform
#'   names during validation. Namespaced transforms such as `stats::lag` do not
#'   need to be registered.
#' @param allow_cycles Whether to retain cycle nodes in the planned DAG instead
#'   of failing when a dependency cycle is detected.
#' @param loaders A named list of functions that load raw series, keyed by
#'   `source` type; see [evaluate_pipeline()].
#'
#' @return A `PipelinePlan` object containing the parsed input, normalized
#'   pipeline, DAG plan, planned outputs, and `needs_inputs`: the raw series
#'   that have no loader and so must be passed via `inputs` when executing.
#' @export
plan_pipeline <- function(path,
                          outputs = NULL,
                          transforms = list(),
                          allow_cycles = FALSE,
                          loaders = list()) {
  assert_loaders(loaders)
  parsed <- read_pipeline(path)
  pipeline <- normalize_pipeline(parsed)
  dag <- build_dag(
    pipeline,
    transforms = transforms,
    allow_cycles = allow_cycles
  )
  planned_outputs <- outputs %||% pipeline$outputs %||% default_output_names(pipeline, dag)

  if (!is.character(planned_outputs)) {
    fail("planned `outputs` must be a character vector")
  }

  unknown_outputs <- setdiff(unique(planned_outputs), dag$nodes)
  if (length(unknown_outputs) > 0L) {
    fail(sprintf(
      "planned outputs are not present in the pipeline: %s",
      paste(unknown_outputs, collapse = ", ")
    ))
  }

  structure(
    list(
      parsed = parsed,
      pipeline = pipeline,
      dag = dag,
      outputs = unique(planned_outputs),
      needs_inputs = unavailable_raw_series(pipeline$series, inputs = list(), loaders = loaders)
    ),
    class = c("PipelinePlan", "tsflow_spec")
  )
}
