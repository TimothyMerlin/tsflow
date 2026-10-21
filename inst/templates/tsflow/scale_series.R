#' Scale a Time Series
#'
#' @param x A time series.
#' @param factor A multiplier.
#' @return The scaled series, retaining its time index.
#' @tsflow transform
scale_series <- function(x, factor = 1) {
  x * factor
}
