#' Validate a Normalized Pipeline
#'
#' Validates duplicate names, cross-references, outputs, and transform
#' resolvability for a normalized pipeline.
#'
#' @param pipeline A `PipelineSpec` object.
#' @param transform_env Environment used to resolve bare transform names during
#'   validation. Namespaced transforms such as `stats::lag` are resolved
#'   directly.
#'
#' @return The input pipeline, invisibly, if validation succeeds.
#' @export
validate_pipeline <- function(pipeline, transform_env = parent.frame()) {
  assert_inherits(pipeline, "PipelineSpec", "pipeline")

  series <- pipeline$series
  if (!is.list(series) || length(series) == 0L) {
    fail("pipeline `series` must be a non-empty list")
  }

  series_names <- vapply(series, function(item) item$name, character(1))
  duplicates <- unique(series_names[duplicated(series_names)])
  if (length(duplicates) > 0L) {
    fail(sprintf(
      "duplicate series names are not allowed: %s",
      paste(duplicates, collapse = ", ")
    ))
  }

  known_names <- unique(series_names)

  if (!is.null(pipeline$outputs)) {
    unknown_outputs <- setdiff(unique(pipeline$outputs), known_names)
    if (length(unknown_outputs) > 0L) {
      fail(sprintf(
        "pipeline `outputs` reference unknown series: %s",
        paste(unknown_outputs, collapse = ", ")
      ))
    }
  }

  dependency_map <- stats::setNames(vector("list", length(series)), series_names)

  for (idx in seq_along(series)) {
    item <- series[[idx]]
    assert_inherits(item, "SeriesSpec", sprintf("series `%s`", item$name))

    if (identical(item$kind, "raw")) {
      dependency_map[[item$name]] <- character()
      next
    }

    if (!identical(item$kind, "derived")) {
      fail(sprintf("series `%s` has unsupported kind `%s`", item$name, item$kind))
    }

    deps <- unique(setdiff(extract_dependencies(item$expression), item$name))
    dependency_map[[item$name]] <- deps

    unknown_refs <- setdiff(deps, known_names)
    if (length(unknown_refs) > 0L) {
      fail(sprintf(
        "series `%s` references unknown series: %s",
        item$name,
        paste(unknown_refs, collapse = ", ")
      ))
    }

    validate_transform_names(item, transform_env)
  }

  invisible(pipeline)
}

validate_transform_names <- function(series, transform_env) {
  transform_names <- collect_transform_names(series$expression)
  if (length(transform_names) == 0L) {
    return(invisible(NULL))
  }

  for (name in transform_names) {
    tryCatch(
      resolve_transform(name, transform_env = transform_env),
      error = function(e) {
        fail(sprintf(
          "series `%s` uses unresolved transform `%s`: %s",
          series$name,
          name,
          conditionMessage(e)
        ))
      }
    )
  }

  invisible(NULL)
}
