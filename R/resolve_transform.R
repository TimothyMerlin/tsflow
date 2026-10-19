#' Resolve Transform Functions
#'
#' Resolves a transform name used in a pipeline to an R function.
#'
#' Bare names such as `scale` are looked up in the `transforms` registry.
#' Namespaced names such as `stats::lag` are resolved directly from the
#' package and do not need to be registered.
#'
#' Any installed package may be used for namespaced transforms. To restrict
#' them, for example when running pipeline files from other people, set
#' `options(tsflow.allowed_packages = c("stats", "tempdisagg"))`.
#'
#' @param name A bare transform name or namespaced function reference such as
#'   `stats::lag`.
#' @param transforms A named list of functions used to resolve bare transform
#'   names.
#'
#' @return An R function.
#' @export
resolve_transform <- function(name, transforms = list()) {
  if (!is_scalar_string(name)) {
    fail("transform name must be a single string")
  }
  assert_transforms(transforms)

  if (grepl("::", name, fixed = TRUE)) {
    if (!grepl("^[A-Za-z][A-Za-z0-9.]*::[A-Za-z][A-Za-z0-9._]*$", name)) {
      fail(sprintf("invalid namespaced transform `%s`", name))
    }

    parts <- strsplit(name, "::", fixed = TRUE)[[1L]]
    pkg <- parts[[1L]]
    fn_name <- parts[[2L]]

    allowed_packages <- getOption("tsflow.allowed_packages")
    if (!is.null(allowed_packages) && !(pkg %in% allowed_packages)) {
      fail(sprintf(
        "package `%s` is not in `options(tsflow.allowed_packages)` for transform `%s`",
        pkg,
        name
      ))
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

  if (!(name %in% names(transforms))) {
    fail(sprintf(
      "transform `%s` is not registered in `transforms`; register it or use a namespaced `pkg::fn` name",
      name
    ))
  }

  transforms[[name]]
}

assert_transforms <- function(transforms) {
  if (!is.list(transforms) || is.object(transforms)) {
    fail("`transforms` must be a named list of functions")
  }
  if (length(transforms) == 0L) {
    return(invisible(transforms))
  }

  nms <- names(transforms)
  if (is.null(nms) || any(is.na(nms) | !nzchar(nms))) {
    fail("`transforms` must be a named list of functions")
  }
  if (anyDuplicated(nms)) {
    fail(sprintf(
      "`transforms` has duplicate names: %s",
      paste(unique(nms[duplicated(nms)]), collapse = ", ")
    ))
  }

  not_functions <- nms[!vapply(transforms, is.function, logical(1))]
  if (length(not_functions) > 0L) {
    fail(sprintf(
      "`transforms` entries must be functions: %s",
      paste(not_functions, collapse = ", ")
    ))
  }

  invisible(transforms)
}

assert_hooks <- function(hooks) {
  supported <- c("binary_op", "postprocess")

  if (!is.list(hooks) || is.object(hooks)) {
    fail("`hooks` must be a named list of functions")
  }
  if (length(hooks) == 0L) {
    return(invisible(hooks))
  }

  nms <- names(hooks)
  if (is.null(nms) || any(is.na(nms) | !nzchar(nms))) {
    fail("`hooks` must be a named list of functions")
  }

  unknown <- setdiff(nms, supported)
  if (length(unknown) > 0L) {
    fail(sprintf(
      "unsupported `hooks`: %s; supported hooks are %s",
      paste(unknown, collapse = ", "),
      paste(supported, collapse = ", ")
    ))
  }

  not_functions <- nms[!vapply(hooks, is.function, logical(1))]
  if (length(not_functions) > 0L) {
    fail(sprintf("`hooks` entries must be functions: %s", paste(not_functions, collapse = ", ")))
  }

  invisible(hooks)
}

# Everything evaluation needs to run transforms, validated once and passed down.
new_runtime <- function(transforms = list(), hooks = list()) {
  assert_transforms(transforms)
  assert_hooks(hooks)
  list(transforms = transforms, hooks = hooks)
}
