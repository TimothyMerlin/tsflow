test_that("the registry roclet collects only tagged functions", {
  skip_if_not_installed("roxygen2")
  registry <- roxygen2::roc_proc_text(registry_roclet(), c(
    "#' Example", "#' @tsflow transform", "scale_series <- function(x) x * 2",
    "hidden_helper <- function(x) x + 1",
    "#' Hook", "#' @tsflow hook postprocess",
    "metadata <- function(result, context) result"
  ))
  expect_equal(registry, list(
    version = 1L, transforms = list(scale_series = "scale_series"),
    hooks = list(postprocess = "metadata")
  ))
})

test_that("invalid registry tags and duplicate registrations fail while documenting", {
  skip_if_not_installed("roxygen2")
  process <- function(code) roxygen2::roc_proc_text(registry_roclet(), code)
  expect_error(process(c("#' Bad", "#' @tsflow hook unknown", "hook <- function(x) x")),
               "must be.*hook binary_op")
  expect_error(process(c("#' Bad", "#' @tsflow transform alias", "fn <- function(x) x")),
               "must be")
  expect_error(process(c("#' Bad", "#' @tsflow transform", "value <- 1")),
               "must document a function")
  expect_error(process(c(
    "#' First", "#' @tsflow hook binary_op", "first <- function(op, left, right) left",
    "#' Second", "#' @tsflow hook binary_op", "second <- function(op, left, right) right"
  )), "duplicate tsflow hooks registration `binary_op`")
  expect_error(process(c(
    "#' Twice", "#' @tsflow transform", "#' @tsflow transform", "fn <- function(x) x"
  )), "duplicate tsflow transforms registration `fn`")
})

test_that("registry output is deterministic and protects handwritten files", {
  skip_if_not_installed("roxygen2")
  dir <- withr::local_tempdir()
  registry <- roxygen2::roc_proc_text(registry_roclet(), c(
    "#' Z", "#' @tsflow transform", "z <- function(x) x",
    "#' A", "#' @tsflow transform", "a <- function(x) x"
  ))
  path <- roxygen2::roclet_output(registry_roclet(), registry, dir)
  expect_equal(names(yaml::read_yaml(path)$transforms), c("a", "z"))
  first <- readLines(path)
  roxygen2::roclet_output(registry_roclet(), registry, dir)
  expect_identical(readLines(path), first)
  writeLines("transforms: handwritten", path)
  expect_error(roxygen2::roclet_output(registry_roclet(), registry, dir),
               "refusing to overwrite")
  expect_identical(readLines(path), "transforms: handwritten")
})

test_that("package_registry gives actionable package and registry errors", {
  expect_error(package_registry(c("stats", "utils")), "single package name")
  expect_error(package_registry("tsflowThisPackageDoesNotExist"), "not installed or loaded")
  expect_error(package_registry("stats"), "has no tsflow registry")
})
