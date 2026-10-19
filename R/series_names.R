#' Series Names in a Pipeline
#'
#' Extracts the series names declared by a pipeline, in definition order.
#'
#' @param x A `PipelineSpec` (from `normalize_pipeline()`), `PipelinePlan`
#'   (from `plan_pipeline()`), or `PipelineResult` (from `execute_pipeline()`).
#' @param kind Optional character vector of series kinds to keep: `"raw"`,
#'   `"derived"`, or both. `NULL` (default) returns every series name.
#'
#' @return Character vector of series names, in definition order. May be empty
#'   when `kind` matches no series.
#' @export
series_names <- function(x, kind = NULL) {
  spec <- as_pipeline_spec(x)

  series <- spec$series
  if (!is.list(series) || length(series) == 0L) {
    fail("pipeline defines no `series`")
  }

  nms <- vapply(series, `[[`, character(1), "name")

  if (is.null(kind)) {
    return(nms)
  }

  valid_kinds <- c("raw", "derived")
  if (!is.character(kind) || !all(kind %in% valid_kinds)) {
    fail("`kind` must be a subset of \"raw\" and \"derived\"")
  }

  keep <- vapply(series, function(s) s[["kind"]] %in% kind, logical(1))
  nms[keep]
}
