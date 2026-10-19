# Raw series come from `inputs` when given there, otherwise from the loader
# registered for their `source` type. Unavailable raw series are "missing":
# they and everything downstream are skipped, but the rest still runs.

assert_loaders <- function(loaders) {
  if (!is.list(loaders) || is.object(loaders)) {
    fail("`loaders` must be a named list of functions")
  }
  if (length(loaders) == 0L) {
    return(invisible(loaders))
  }

  nms <- names(loaders)
  if (is.null(nms) || any(is.na(nms) | !nzchar(nms))) {
    fail("`loaders` must be a named list of functions")
  }
  if (anyDuplicated(nms)) {
    fail(sprintf("`loaders` has duplicate names: %s", paste(unique(nms[duplicated(nms)]), collapse = ", ")))
  }
  if ("injected" %in% nms) {
    fail("`loaders` cannot define `injected`; injected series always come from `inputs`")
  }

  not_functions <- nms[!vapply(loaders, is.function, logical(1))]
  if (length(not_functions) > 0L) {
    fail(sprintf("`loaders` entries must be functions: %s", paste(not_functions, collapse = ", ")))
  }

  invisible(loaders)
}

# Raw series that neither `inputs` nor a loader can provide.
unavailable_raw_series <- function(series_list, inputs, loaders) {
  raw <- Filter(function(series) identical(series$kind, "raw"), series_list)
  names <- vapply(raw, `[[`, character(1), "name")
  available <- vapply(raw, function(series) {
    has_input(inputs, series$name) || series$source$type %in% names(loaders)
  }, logical(1))
  names[!available]
}

has_input <- function(inputs, name) {
  name %in% names(inputs) && !is.null(inputs[[name]])
}

# Returns list(status = "ok" | "missing" | "error", value, error).
load_raw_series <- function(series, inputs, loaders) {
  name <- series$name

  if (has_input(inputs, name)) {
    value <- inputs[[name]]
  } else if (series$source$type %in% names(loaders)) {
    type <- series$source$type
    value <- tryCatch(
      loaders[[type]](series$source$config, name),
      error = function(e) {
        structure(
          list(message = sprintf("loader `%s` failed: %s", type, conditionMessage(e))),
          class = "tsflow_load_error"
        )
      }
    )
    if (inherits(value, "tsflow_load_error")) {
      return(list(status = "error", error = value$message))
    }
    if (is.null(value)) {
      return(list(status = "missing"))
    }
  } else {
    return(list(status = "missing"))
  }

  if (!(stats::is.ts(value) || inherits(value, "xts"))) {
    return(list(
      status = "error",
      error = sprintf("raw input `%s` must be an R `ts` or `xts` object", name)
    ))
  }

  list(status = "ok", value = value)
}

load_raw_inputs <- function(raw_names, series_map, inputs, loaders) {
  values <- list()
  missing <- character()
  errors <- list()

  for (name in raw_names) {
    loaded <- load_raw_series(series_map[[name]], inputs, loaders)
    if (identical(loaded$status, "ok")) {
      values[[name]] <- loaded$value
    } else if (identical(loaded$status, "missing")) {
      missing <- c(missing, name)
    } else {
      errors[[name]] <- loaded$error
    }
  }

  list(values = values, missing = missing, errors = errors)
}

# Placeholder value for a missing raw series and everything computed from it
# when running under targets, where a target can't simply be skipped.
new_missing_input <- function(names) {
  structure(list(names = unique(names)), class = "tsflow_missing_input")
}

is_missing_input <- function(x) {
  inherits(x, "tsflow_missing_input")
}

#' @export
print.tsflow_missing_input <- function(x, ...) {
  cat(sprintf("<missing input: %s>\n", paste(x$names, collapse = ", ")))
  invisible(x)
}
