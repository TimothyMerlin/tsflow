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

# MD5 of a character vector, one element per line. Goes through a temporary
# file because `tools::md5sum()` only hashes files before R 4.5; the file is
# written in binary mode so lines end in "\n" on Windows too.
md5_text <- function(lines) {
  file <- tempfile()
  on.exit(unlink(file), add = TRUE)
  con <- file(file, open = "wb")
  writeLines(lines, con, useBytes = TRUE)
  close(con)
  unname(tools::md5sum(file))
}

# Hash of a series' data: class, dimensions, frequency, column names, time
# index and values. Built from the numbers rather than the serialized object,
# so the same data gives the same hash in any session or R version.
series_hash <- function(x) {
  numbers <- function(v) sprintf("%.17g", as.numeric(v))
  md5_text(c(
    paste(class(x), collapse = ","),
    paste(dim(x), collapse = ","),
    if (stats::is.ts(x)) numbers(stats::frequency(x)),
    paste(colnames(x), collapse = ","),
    numbers(stats::time(x)),
    numbers(x)
  ))
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
