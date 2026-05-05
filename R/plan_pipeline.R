#' Plan a Pipeline Without Executing It
#'
#' Reads, normalizes, validates, and builds a DAG plan for a pipeline without
#' executing any transforms.
#'
#' @param path Path to a pipeline definition file.
#' @param outputs Optional output names to plan for. When `NULL`, pipeline
#'   outputs are used if present, otherwise all nodes are included.
#' @param transform_env Environment used to resolve bare transform names during
#'   validation.
#' @param allow_cycles Whether to retain cycle nodes in the planned DAG instead
#'   of failing when a dependency cycle is detected.
#'
#' @return A `PipelinePlan` object containing the parsed input, normalized
#'   pipeline, DAG plan, and planned outputs.
#' @export
plan_pipeline <- function(path,
                          outputs = NULL,
                          transform_env = parent.frame(),
                          allow_cycles = FALSE) {
  parsed <- read_pipeline(path)
  pipeline <- normalize_pipeline(parsed)
  dag <- build_dag(
    pipeline,
    transform_env = transform_env,
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
      outputs = unique(planned_outputs)
    ),
    class = c("PipelinePlan", "tsflow_spec")
  )
}
