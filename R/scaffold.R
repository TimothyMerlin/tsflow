#' Create a tsflow Pipeline Package
#'
#' Creates an R package with a tagged example transform, a generated registry,
#' sample inputs, CSV pipeline and sidecar, a `run_pipeline()` entry point,
#' tests, and a getting-started README. Uses `usethis::create_package()`.
#' Package creation does not change the working directory or open an editor.
#'
#' @param path Path for the new package. Must not already exist.
#' @param targets Also add `_targets.R` to run the same pipeline with targets.
#'   Direct execution remains available through `run.R` and `run_pipeline()`.
#' @return The package path, invisibly.
#' @export
create_tsflow_package <- function(path, targets = FALSE) {
  assert_scaffold_flag(targets)
  if (!is_scalar_string(path) || !nzchar(path)) fail("`path` must be a single directory path")
  if (file.exists(path)) fail(sprintf("`path` already exists: %s", path))
  require_setup_package("usethis")
  require_setup_package("roxygen2")
  usethis::create_package(path, rstudio = FALSE, open = FALSE)
  use_tsflow(path, targets = targets)
  invisible(normalizePath(path, mustWork = TRUE))
}

#' Set Up a tsflow Pipeline in a Project or Package
#'
#' Adds a runnable example pipeline to an existing project, or creates a project
#' directory if needed. In a package, also configures registry generation,
#' creates a `run_pipeline()` entry point and tests, and documents the package.
#' An ordinary project uses an explicit transform list and requires no package
#' configuration. Existing starter files are always preserved; rerun with
#' `targets = TRUE` to add targets execution later.
#'
#' @param path Project or package root. Defaults to the current directory.
#' @param targets Also add `_targets.R`. The package is optional at setup time;
#'   install `targets` before running `targets::tar_make()`.
#' @return Paths of newly created files, invisibly.
#' @export
use_tsflow <- function(path = ".", targets = FALSE) {
  assert_scaffold_flag(targets)
  if (!is_scalar_string(path) || !nzchar(path)) fail("`path` must be a single directory path")
  if (file.exists(path) && !dir.exists(path)) fail("`path` must be a directory")
  package <- file.exists(file.path(path, "DESCRIPTION"))
  pkg <- if (package) scaffold_package_name(path) else ""
  if (package) {
    require_setup_package("roxygen2")
    configure_tsflow_package(path, targets)
  }

  pipeline_dir <- if (package) "inst/extdata" else "."
  prefix <- if (package) "package" else "project"
  readme <- file.path(path, "README.md")
  guide <- if (file.exists(readme) &&
               !any(readLines(readme, warn = FALSE) == "<!-- tsflow setup guide -->")) {
    "TSFLOW.md"
  } else {
    "README.md"
  }
  files <- c(
    "R/scale_series.R" = "scale_series.R",
    "R/example_inputs.R" = "example_inputs.R",
    "run.R" = paste0(prefix, "-run.R")
  )
  files[guide] <- paste0(prefix, "-README.md")
  files[file.path(pipeline_dir, "pipeline.csv")] <- "pipeline.csv"
  files[file.path(pipeline_dir, "pipeline.meta.yml")] <- "pipeline.meta.yml"
  if (package) {
    files["R/run_pipeline.R"] <- "package-run_pipeline.R"
    files["tests/testthat/test-pipeline.R"] <- "test-pipeline.R"
  }
  if (targets) files["_targets.R"] <- paste0(prefix, "-_targets.R")

  created <- character()
  for (relative_path in names(files)) {
    destination <- file.path(path, relative_path)
    if (file.exists(destination)) {
      cli::cli_inform(escape_cli_braces(sprintf("Keeping existing %s", destination)))
      next
    }
    lines <- read_scaffold_template(files[[relative_path]], pkg)
    dir.create(dirname(destination), recursive = TRUE, showWarnings = FALSE)
    writeLines(lines, destination, useBytes = TRUE)
    created <- c(created, destination)
  }
  if (package) {
    test_runner <- file.path(path, "tests", "testthat.R")
    if (!file.exists(test_runner)) {
      writeLines(c("library(testthat)", sprintf("library(%s)", pkg),
                   sprintf('test_check("%s")', pkg)), test_runner)
      created <- c(created, test_runner)
    }
    append_scaffold_lines(file.path(path, ".Rbuildignore"), c("^run\\.R$", "^_targets\\.R$", "^_targets$", "^_tsflow$"))
    roxygen2::roxygenise(path)
  }
  append_scaffold_lines(file.path(path, ".gitignore"), c("_targets/", "_tsflow/"))
  cli::cli_inform(escape_cli_braces(c(
    "tsflow setup is ready.",
    "i" = sprintf("Run commands from %s", normalizePath(path, mustWork = TRUE)),
    "i" = 'Run the example: source("run.R")',
    "i" = if (package) "Complete DESCRIPTION and choose a package license before sharing." else "The setup guide describes the files and the first edits.",
    "i" = if (package) 'Add a transform: tsflow::use_tsflow_transform("my_transform")' else "Edit pipeline.csv and R/example_inputs.R.",
    "i" = if (package) "After editing functions: roxygen2::roxygenise(); testthat::test_local()" else "Register new functions in run.R's transforms list.",
    "i" = if (targets) "Run with targets: targets::tar_make()" else "Add targets later: tsflow::use_tsflow(targets = TRUE)",
    "i" = sprintf("See %s for the walkthrough.", guide)
  )))
  invisible(created)
}

#' Add a Transform to a tsflow Package
#'
#' Writes a function tagged `@tsflow transform` and a starter test. Implement
#' the function and update the test, then run `roxygen2::roxygenise()` to add it
#' to the registry. Neither file may already exist.
#'
#' @param name A syntactic R function name.
#' @param path Package root.
#' @return Paths of the new files, invisibly.
#' @export
use_tsflow_transform <- function(name, path = ".") {
  scaffold_package_name(path)
  if (!is_scalar_string(name) || !is_syntactic_name(name)) {
    fail("`name` must be a syntactic R function name")
  }
  write_tsflow_stub(path, name, c(
    sprintf("#' %s", name), "#'", "#' @param x An incoming time series.",
    "#' @return A transformed time series.", "#' @tsflow transform",
    sprintf("%s <- function(x) {", name), "  # TODO: implement the transform.", "  x", "}"
  ), c(
    sprintf('test_that("%s preserves the input time index", {', name),
    "  x <- stats::ts(1:4, start = c(2020, 1), frequency = 4)",
    "  # TODO: add expected values and any metadata your transform must preserve.",
    sprintf("  expect_equal(stats::tsp(%s(x)), stats::tsp(x))", name), "})"
  ))
}

#' Add an Advanced Evaluation Hook to a tsflow Package
#'
#' Hooks affect every step of their kind and are usually unnecessary. A
#' `postprocess` hook receives `(result, context)` after each transform. A
#' `binary_op` hook receives `(op, left, right)` and replaces default arithmetic,
#' including its alignment checks. Add your own checks when replacing it.
#' Run `roxygen2::roxygenise()` after editing the stub to update the registry.
#'
#' @param slot Either `"binary_op"` or `"postprocess"`.
#' @param path Package root.
#' @return Paths of the new files, invisibly.
#' @export
use_tsflow_hook <- function(slot, path = ".") {
  scaffold_package_name(path)
  if (!is_scalar_string(slot) || !slot %in% c("binary_op", "postprocess")) {
    fail("`slot` must be `binary_op` or `postprocess`")
  }
  name <- paste0("tsflow_", slot)
  if (slot == "postprocess") {
    code <- c(
      "#' Postprocess Every Transform Result", "#'",
      "#' Usually unnecessary; customize only for shared metadata handling.",
      "#' @param result The transform result.", "#' @param context Evaluation context.",
      "#' @return The postprocessed result.", "#' @tsflow hook postprocess",
      sprintf("%s <- function(result, context) {", name),
      "  # TODO: add metadata handling if needed.", "  result", "}"
    )
    test <- c('test_that("postprocessing preserves a time series", {',
              "  x <- stats::ts(1:4)",
              sprintf("  expect_equal(%s(result = x, context = list()), x)", name), "})")
  } else {
    code <- c(
      "#' Customize Pipeline Arithmetic", "#'",
      "#' Replaces default arithmetic AND alignment checks for all binary steps.",
      "#' @param op One of add, sub, mul, div.", "#' @param left Left operand.",
      "#' @param right Right operand.", "#' @return The arithmetic result.",
      "#' @tsflow hook binary_op", sprintf("%s <- function(op, left, right) {", name),
      "  # TODO: implement alignment checks and metadata handling for your series.",
      "  switch(op, add = left + right, sub = left - right,",
      '         mul = left * right, div = left / right, stop("Unknown operation: ", op))', "}"
    )
    test <- c('test_that("the arithmetic hook adds aligned series", {',
              "  x <- stats::ts(1:4)",
              sprintf('  expect_equal(%s(op = "add", left = x, right = x), x * 2)', name), "})")
  }
  write_tsflow_stub(path, name, code, test)
}

require_setup_package <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    fail(sprintf('package `%s` is required for this setup; install it with install.packages("%s")', pkg, pkg))
  }
}

assert_scaffold_flag <- function(targets) {
  if (!is_scalar_flag(targets)) fail("`targets` must be TRUE or FALSE")
}

scaffold_package_name <- function(path) {
  if (!is_scalar_string(path) || !dir.exists(path)) fail("`path` must be an existing package root")
  description <- file.path(path, "DESCRIPTION")
  if (!file.exists(description)) fail("`path` must contain a package DESCRIPTION")
  fields <- read.dcf(description)
  if (!"Package" %in% colnames(fields) || !is_syntactic_name(unname(fields[1L, "Package"]))) {
    fail("DESCRIPTION must contain a valid Package name")
  }
  unname(fields[1L, "Package"])
}

configure_tsflow_package <- function(path, targets) {
  description <- file.path(path, "DESCRIPTION")
  fields <- as.list(read.dcf(description)[1L, ])
  add_dependency <- function(field, pkg, version = NULL) {
    entries <- trimws(strsplit(fields[[field]] %||% "", ",", fixed = TRUE)[[1L]])
    entries <- entries[nzchar(entries)]
    if (!pkg %in% sub("[[:space:]]*\\(.*$", "", entries)) {
      entry <- if (is.null(version)) pkg else sprintf("%s (>= %s)", pkg, version)
      entries <- c(entries, entry)
    }
    fields[[field]] <<- paste(entries, collapse = ", ")
  }
  add_dependency("Imports", "tsflow")
  add_dependency("Imports", "stats")
  add_dependency("Suggests", "testthat", version = "3.0.0")
  for (pkg in c("pkgload", "roxygen2", if (targets) "targets")) {
    add_dependency("Suggests", pkg)
  }
  fields[["Config/testthat/edition"]] <- fields[["Config/testthat/edition"]] %||% "3"
  options <- roxygen2::load_options(path)
  config <- if (is.null(fields$Roxygen)) list() else eval(str2lang(fields$Roxygen), baseenv())
  config$packages <- unique(c(options$packages, "tsflow"))
  config$roclets <- unique(c(options$roclets, "tsflow::registry_roclet"))
  fields$Roxygen <- paste(deparse(config, width.cutoff = 80L), collapse = " ")
  write.dcf(as.data.frame(fields, stringsAsFactors = FALSE), description)
}

read_scaffold_template <- function(name, pkg) {
  source <- system.file("templates", "tsflow", name, package = "tsflow", mustWork = TRUE)
  gsub("{{package}}", pkg, readLines(source, warn = FALSE), fixed = TRUE)
}

append_scaffold_lines <- function(path, lines) {
  existing <- if (file.exists(path)) readLines(path, warn = FALSE) else character()
  missing <- setdiff(lines, existing)
  if (length(missing) > 0L) writeLines(c(existing, missing), path, useBytes = TRUE)
}

write_tsflow_stub <- function(path, name, code, test) {
  paths <- file.path(path, c(file.path("R", paste0(name, ".R")),
                            file.path("tests", "testthat", paste0("test-", name, ".R"))))
  if (any(file.exists(paths))) {
    fail(sprintf("refusing to overwrite existing files: %s", paste(paths[file.exists(paths)], collapse = ", ")))
  }
  dir.create(dirname(paths[[1L]]), recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(paths[[2L]]), recursive = TRUE, showWarnings = FALSE)
  writeLines(code, paths[[1L]], useBytes = TRUE)
  writeLines(test, paths[[2L]], useBytes = TRUE)
  cli::cli_inform(c(sprintf("Added %s and its starter test.", name),
                   "i" = "Implement the function, update its test, then run roxygen2::roxygenise()."))
  invisible(paths)
}
