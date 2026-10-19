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
#' @param transforms A named list of functions used to resolve bare transform
#'   names during validation and execution. Namespaced transforms such as
#'   `stats::lag` do not need to be registered.
#' @param hooks Optional named list of evaluation hooks; see
#'   [evaluate_pipeline()].
#' @param progress Whether to show execution progress in interactive sessions.
#'   Defaults to `getOption("tsflow.progress", TRUE)`.
#' @param error_mode Execution failure mode. `"fail"` stops on the first node
#'   error. `"collect"` continues evaluating unaffected nodes and returns
#'   node-level errors and skipped nodes in the result.
#' @param store Optional directory for caching results between runs. When set,
#'   the pipeline is run with `targets` (see [tar_tsflow()]) and only steps
#'   whose inputs, parameters or transforms changed are recomputed. Defaults to
#'   `getOption("tsflow.store")`, so `options(tsflow.store = "_tsflow_cache")`
#'   turns caching on for a whole script; `NULL` runs in memory as usual.
#'
#' @details With `store`, each pipeline file gets its own subfolder of `store`
#'   (named after the file), so pipelines that share series names don't
#'   overwrite each other's results. Changes to transforms and to the
#'   workspace functions and values they use are detected; changes inside
#'   packages are detected through the package version. The result has the
#'   usual structure plus `rebuilt`, the series recomputed in this run, and
#'   `store`, the subfolder used. Two R processes must not use the same
#'   `store` at the same time. Requires the `targets` and `codetools`
#'   packages.
#'
#' @return A `PipelineResult` object.
#' @export
execute_pipeline <- function(path,
                             inputs,
                             outputs = NULL,
                             transforms = list(),
                             hooks = list(),
                             progress = getOption("tsflow.progress", TRUE),
                             error_mode = c("fail", "collect"),
                             store = getOption("tsflow.store")) {
  error_mode <- match.arg(error_mode)

  if (!is.null(store)) {
    return(execute_pipeline_cached(
      path = path,
      inputs = inputs,
      outputs = outputs,
      transforms = transforms,
      hooks = hooks,
      progress = progress,
      error_mode = error_mode,
      store = store
    ))
  }

  parsed <- read_pipeline(path)
  pipeline <- normalize_pipeline(parsed)

  evaluate_pipeline(
    pipeline = pipeline,
    inputs = inputs,
    outputs = outputs %||% pipeline$outputs %||% NULL,
    transforms = transforms,
    hooks = hooks,
    progress = progress,
    error_mode = error_mode
  )
}

#' @rdname execute_pipeline
#' @export
run_pipeline <- function(path,
                         inputs,
                         outputs = NULL,
                         transforms = list(),
                         hooks = list(),
                         progress = getOption("tsflow.progress", TRUE),
                         error_mode = c("fail", "collect"),
                         store = getOption("tsflow.store")) {
  error_mode <- match.arg(error_mode)
  execute_pipeline(
    path = path,
    inputs = inputs,
    outputs = outputs,
    transforms = transforms,
    hooks = hooks,
    progress = progress,
    error_mode = error_mode,
    store = store
  )
}
