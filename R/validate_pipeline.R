#' Validate a Normalized Pipeline
#'
#' Validates series names, duplicate names, cross-references, outputs, and
#' transform resolvability for a normalized pipeline.
#'
#' Series names must be syntactically valid R names that start with a letter
#' (letters, digits, `.` and `_`, not a reserved word) and must not contain
#' `..`.
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

  validate_series_names(series_names)

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
      msg <- sprintf(
        "series `%s` references unknown series: %s",
        item$name,
        paste(unknown_refs, collapse = ", ")
      )

      # Bare strings in step args are series refs, which surprises authors who
      # meant a text value, so point them at the explicit literal form.
      unknown_arg_refs <- intersect(unknown_refs, extract_arg_dependencies(item$expression))
      if (length(unknown_arg_refs) > 0L) {
        msg <- c(msg, "i" = sprintf(
          "Strings in step arguments are series references. For a text value, use `{literal: %s}`.",
          unknown_arg_refs[[1L]]
        ))
      }

      fail(msg)
    }

    validate_transform_names(item, transform_env)
  }

  invisible(pipeline)
}

# Series names become R symbols in parsed expressions and targets names, and
# `..` is reserved for per-step target names.
validate_series_names <- function(series_names) {
  invalid <- unique(series_names[
    make.names(series_names) != series_names |
      !grepl("^[A-Za-z]", series_names) |
      grepl("..", series_names, fixed = TRUE)
  ])
  if (length(invalid) == 0L) {
    return(invisible(NULL))
  }

  suggested <- gsub("\\.{2,}", ".", make.names(invalid))
  suggested <- ifelse(grepl("^[A-Za-z]", suggested), suggested, paste0("X", suggested))
  fail(c(
    sprintf("series names must be valid R names: %s", paste(invalid, collapse = ", ")),
    "i" = "Use letters, digits, `.` and `_`, starting with a letter, without `..`.",
    "i" = sprintf("Suggested: %s", paste(suggested, collapse = ", "))
  ))
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
