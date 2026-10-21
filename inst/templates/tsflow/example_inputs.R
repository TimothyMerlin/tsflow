#' Sample Inputs for the Example Pipeline
#'
#' Replace these sample values with your own data-loading code. The list names
#' must match the raw series names used in the pipeline CSV.
#' @return A named list of time series.
#' @export
example_inputs <- function() {
  list(raw = stats::ts(1:4, start = c(2020, 1), frequency = 4))
}
