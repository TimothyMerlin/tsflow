# A package `tsflowdevfixture` loaded with pkgload::load_all(), unloaded when
# `env` exits. Returns a function that rewrites its R code and reloads it.
# The exported `transform()` reaches `helper()` through S3 dispatch, so tests
# cover code that a function's own body doesn't name.
local_dev_package <- function(env = parent.frame()) {
  package <- withr::local_tempdir(.local_envir = env)
  dir.create(file.path(package, "R"))
  writeLines(
    c(
      "Package: tsflowdevfixture", "Version: 0.0.1", "Title: Fixture",
      "Description: Fixture.", "License: MIT", "Encoding: UTF-8"
    ),
    file.path(package, "DESCRIPTION")
  )
  writeLines("export(transform)", file.path(package, "NAMESPACE"))
  withr::defer(pkgload::unload("tsflowdevfixture"), envir = env)

  function(multiplier = 2, unused = 1) {
    writeLines(
      c(
        "transform <- function(x) apply_fixture(structure(list(x), class = 'fixture'))",
        "apply_fixture <- function(x) UseMethod('apply_fixture')",
        "apply_fixture.fixture <- function(x) helper(x[[1L]])",
        sprintf("helper <- function(x) x * %s", multiplier),
        sprintf("unused <- function(x) x + %s", unused)
      ),
      file.path(package, "R", "functions.R")
    )
    pkgload::load_all(package, quiet = TRUE, export_all = FALSE)
    invisible()
  }
}
