#' Resolve Transform Functions
#'
#' Resolves transform references in the R-first MVP runtime.
#'
#' @param name A bare function name or namespaced function reference such as
#'   `stats::lag`.
#' @param transform_env Environment used to resolve bare function names.
#'
#' @return An R function.
#' @export
resolve_transform <- function(name, transform_env = parent.frame()) {
  if (!is_scalar_string(name)) {
    fail("transform name must be a single string")
  }

  if (grepl("::", name, fixed = TRUE)) {
    if (!grepl("^[A-Za-z][A-Za-z0-9.]*::[A-Za-z][A-Za-z0-9._]*$", name)) {
      fail(sprintf("invalid namespaced transform `%s`", name))
    }
  }

  parts <- strsplit(name, "::", fixed = TRUE)[[1]]

  if (length(parts) == 2L) {
    pkg <- parts[[1L]]
    fn_name <- parts[[2L]]

    if (!is_scalar_string(pkg) || !nzchar(pkg) || !is_scalar_string(fn_name) || !nzchar(fn_name)) {
      fail(sprintf("invalid namespaced transform `%s`", name))
    }

    if (!requireNamespace(pkg, quietly = TRUE)) {
      fail(sprintf("package `%s` is not installed for transform `%s`", pkg, name))
    }

    fn <- getExportedValue(pkg, fn_name)
    if (!is.function(fn)) {
      fail(sprintf("transform `%s` did not resolve to a function", name))
    }
    return(fn)
  }

  if (length(parts) != 1L) {
    fail(sprintf("invalid transform name `%s`", name))
  }

  if (!exists(name, envir = transform_env, mode = "function", inherits = TRUE)) {
    fail(sprintf("transform `%s` could not be resolved", name))
  }

  get(name, envir = transform_env, mode = "function", inherits = TRUE)
}
