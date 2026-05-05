#' Execute a Pipeline From a File
#'
#' Convenience entry point that reads, normalizes, validates, and evaluates a
#' pipeline from a YAML or CSV definition file. `run_pipeline()` is retained as
#' a compatibility alias for `execute_pipeline()`.
#'
#' @param path Path to a pipeline definition file.
#' @param inputs A named list of preloaded raw series values. For MVP these are
#'   expected to be R `ts` objects keyed by raw series name.
#' @param outputs Optional output names to materialize. When `NULL`, pipeline
#'   outputs are used if present.
#' @param transform_env Environment used to resolve bare transform names during
#'   validation and execution.
#' @param progress Whether to show execution progress in interactive sessions.
#'   Defaults to `getOption("tsflow.progress", TRUE)`.
#' @param error_mode Execution failure mode. `"fail"` stops on the first node
#'   error. `"collect"` continues evaluating unaffected nodes and returns
#'   node-level errors and skipped nodes in the result.
#'
#' @return A `PipelineResult` object.
#' @export
execute_pipeline <- function(path,
                             inputs,
                             outputs = NULL,
                             transform_env = parent.frame(),
                             progress = getOption("tsflow.progress", TRUE),
                             error_mode = c("fail", "collect")) {
  error_mode <- match.arg(error_mode)
  parsed <- read_pipeline(path)
  pipeline <- normalize_pipeline(parsed)

  evaluate_pipeline(
    pipeline = pipeline,
    inputs = inputs,
    outputs = outputs %||% pipeline$outputs %||% NULL,
    transform_env = transform_env,
    progress = progress,
    error_mode = error_mode
  )
}

#' @rdname execute_pipeline
#' @export
run_pipeline <- function(path,
                         inputs,
                         outputs = NULL,
                         transform_env = parent.frame(),
                         progress = getOption("tsflow.progress", TRUE),
                         error_mode = c("fail", "collect")) {
  error_mode <- match.arg(error_mode)
  execute_pipeline(
    path = path,
    inputs = inputs,
    outputs = outputs,
    transform_env = transform_env,
    progress = progress,
    error_mode = error_mode
  )
}
