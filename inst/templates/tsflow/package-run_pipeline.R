#' Run the Pipeline
#'
#' @param inputs A named list of raw series, as returned by `example_inputs()`.
#' @param path Path to the pipeline CSV or YAML file.
#' @param error_mode Error handling passed to `tsflow::execute_pipeline()`.
#' @param ... Further arguments passed to `tsflow::execute_pipeline()`.
#' @return A tsflow pipeline result.
#' @export
run_pipeline <- function(
    inputs = example_inputs(),
    path = system.file("extdata", "pipeline.csv", package = "{{package}}"),
    error_mode = "collect", ...) {
  registry <- tsflow::package_registry("{{package}}")
  tsflow::execute_pipeline(
    path, inputs = inputs, transforms = registry$transforms,
    hooks = registry$hooks, error_mode = error_mode, ...
  )
}
