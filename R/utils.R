`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

fail <- function(...) {
  cli::cli_abort(escape_cli_braces(c(...)), call = NULL)
}

warn <- function(...) {
  cli::cli_warn(escape_cli_braces(c(...)), call = NULL)
  invisible(NULL)
}

# Messages embed user-supplied names and values; doubling braces stops cli from
# evaluating them as glue expressions.
escape_cli_braces <- function(msg) {
  gsub("}", "}}", gsub("{", "{{", msg, fixed = TRUE), fixed = TRUE)
}

is_scalar_string <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x)
}

is_scalar_number <- function(x) {
  (is.integer(x) || is.double(x)) && length(x) == 1L && !is.na(x)
}

is_scalar_flag <- function(x) {
  is.logical(x) && length(x) == 1L && !is.na(x)
}

is_scalar_value <- function(x) {
  is.null(x) || is_scalar_string(x) || is_scalar_number(x) || is_scalar_flag(x)
}

is_literal_value <- function(x) {
  if (is.null(x)) {
    return(TRUE)
  }

  if (is.character(x) || is.integer(x) || is.double(x) || is.logical(x)) {
    return(length(x) > 0L && !any(is.na(x)))
  }

  FALSE
}

assert_list <- function(x, what) {
  if (!is.list(x)) {
    fail(sprintf("%s must be a list", what))
  }
}

assert_named_list <- function(x, what) {
  assert_list(x, what)
  nms <- names(x)
  if (is.null(nms)) {
    fail(sprintf("%s must be a named list", what))
  }
}

assert_inherits <- function(x, class_name, what) {
  if (!inherits(x, class_name)) {
    fail(sprintf("%s must inherit from `%s`", what, class_name))
  }
}

as_pipeline_spec <- function(x) {
  if (inherits(x, "PipelineSpec")) {
    return(x)
  }
  if (inherits(x, c("PipelinePlan", "PipelineResult"))) {
    return(x$pipeline)
  }
  fail("`x` must be a `PipelineSpec`, `PipelinePlan`, or `PipelineResult`")
}
