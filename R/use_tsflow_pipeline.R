#' Add a Named Pipeline to a Project or Package
#'
#' Adds a starter calculation with a raw input named `raw`, a shared `factor`
#' parameter, and an output named after the pipeline. CSV pipelines have a
#' `.meta.yml` sidecar; YAML pipelines keep their parameters and outputs in the
#' same file. The starter runs without custom transforms.
#'
#' In a tsflow package, files go under `inst/extdata/` and a validation test is
#' added under `tests/testthat/`. Set up the package first with
#' [create_tsflow_package()] or [use_tsflow()]. In an ordinary project, files
#' go in the project root and no package test is added.
#'
#' Existing files with the same pipeline name, including another format or
#' sidecar, cause an error before any files are written. Select the new pipeline
#' path explicitly in your runner or `_targets.R`; this helper does not change
#' the package's default pipeline or execution scripts.
#'
#' @param name A pipeline file stem, starting with a letter and containing only
#'   letters, numbers, dots, underscores, or hyphens.
#' @param path Existing project or package root.
#' @param format Either `"csv"` (the default) or `"yaml"` (a `.yml` file).
#' @return Paths of the new files, invisibly.
#' @export
use_tsflow_pipeline <- function(name, path = ".", format = c("csv", "yaml")) {
  if (!is_scalar_string(name) || !grepl("^[A-Za-z][A-Za-z0-9._-]*$", name)) {
    fail("`name` must start with a letter and contain only letters, numbers, dots, underscores, or hyphens")
  }
  if (!is_scalar_string(path) || !dir.exists(path)) {
    fail("`path` must be an existing project or package directory")
  }
  format <- match.arg(format)
  package <- file.exists(file.path(path, "DESCRIPTION"))
  pkg <- if (package) scaffold_package_name(path) else ""
  if (package && !file.exists(file.path(path, "inst", "tsflow", "registry.yml"))) {
    fail("set up the package with `use_tsflow()` before adding a named pipeline")
  }
  directory <- if (package) file.path(path, "inst", "extdata") else path
  extension <- if (format == "csv") "csv" else "yml"
  filename <- paste0(name, ".", extension)
  templates <- stats::setNames(
    if (format == "csv") "named-pipeline.csv" else "named-pipeline.yml",
    file.path(directory, filename)
  )
  if (format == "csv") {
    templates[file.path(directory, paste0(name, ".meta.yml"))] <- "named-pipeline.meta.yml"
  }
  test_path <- file.path(path, "tests", "testthat", paste0("test-pipeline-", name, ".R"))
  if (package) templates[test_path] <- "test-named-pipeline.R"

  # A shared stem would make a sidecar apply to both pipeline formats.
  possible <- c(file.path(directory, paste0(name, c(".csv", ".yml", ".yaml", ".meta.yml", ".meta.yaml"))),
                if (package) test_path)
  conflicts <- possible[file.exists(possible)]
  if (length(conflicts) > 0L) {
    fail(sprintf("refusing to overwrite or share an existing pipeline name: %s", paste(conflicts, collapse = ", ")))
  }
  contents <- lapply(templates, function(template) {
    lines <- read_scaffold_template(template, pkg)
    replacements <- c(pipeline = name, output = make.names(paste0(name, "_output")), file = filename)
    for (key in names(replacements)) {
      lines <- gsub(paste0("{{", key, "}}"), replacements[[key]], lines, fixed = TRUE)
    }
    lines
  })
  for (destination in names(contents)) {
    dir.create(dirname(destination), recursive = TRUE, showWarnings = FALSE)
    writeLines(contents[[destination]], destination, useBytes = TRUE)
  }
  cli::cli_inform(escape_cli_braces(c(
    sprintf("Added the %s pipeline.", name),
    "i" = sprintf("Edit %s to define its steps.", file.path(directory, filename)),
    "i" = 'The starter expects inputs = list(raw = stats::ts(1:4)).',
    "i" = if (package) "Run testthat::test_local() to validate it against the package registry." else "Run it with tsflow::execute_pipeline().",
    "i" = "Select this pipeline path explicitly in your runner or _targets.R."
  )))
  invisible(names(contents))
}
