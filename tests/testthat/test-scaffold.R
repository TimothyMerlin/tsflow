# A fixed, valid package name in a fresh temporary parent directory.
local_scaffold_package <- function(targets = FALSE, .local_envir = parent.frame()) {
  skip_if_not_installed("usethis")
  skip_if_not_installed("roxygen2")
  root <- withr::local_tempdir(.local_envir = .local_envir)
  path <- file.path(root, "tsflowstartertest")
  create_tsflow_package(path, targets = targets)
  pkgload::load_all(path, quiet = TRUE)
  withr::defer(pkgload::unload("tsflowstartertest"), envir = .local_envir)
  path
}

test_that("ordinary projects run the starter without a package or targets", {
  dir <- file.path(withr::local_tempdir(), "analysis")
  original_wd <- getwd()
  use_tsflow(dir)
  expect_identical(getwd(), original_wd)
  expect_false(file.exists(file.path(dir, "DESCRIPTION")))
  expect_false(file.exists(file.path(dir, "_targets.R")))
  withr::local_dir(dir)
  envir <- new.env(parent = globalenv())
  sys.source("run.R", envir = envir)
  expect_equal(envir$result$values$adjusted, envir$example_inputs()$raw * 20)
  expect_length(envir$result$errors, 0L)
  expect_length(envir$result$missing_inputs, 0L)
})

test_that("targets can be added later without overwriting project files", {
  skip_if_not_installed("targets")
  dir <- withr::local_tempdir()
  use_tsflow(dir)
  writeLines(c(readLines(file.path(dir, "pipeline.meta.yml")), "# My settings"),
             file.path(dir, "pipeline.meta.yml"))
  before <- tools::md5sum(list.files(dir, recursive = TRUE, full.names = TRUE))
  use_tsflow(dir, targets = TRUE)
  expect_identical(tools::md5sum(names(before)), before)
  expect_true(file.exists(file.path(dir, "_targets.R")))
  withr::local_dir(dir)
  targets::tar_make(callr_function = NULL, reporter = "silent")
  expect_equal(as.numeric(targets::tar_read(adjusted)), c(20, 40, 60, 80))
  expect_length(targets::tar_outdated(callr_function = NULL), 0L)
})

test_that("package starters generate a namespace registry and run their own tests", {
  path <- local_scaffold_package()
  expect_false(file.exists(file.path(path, "_targets.R")))
  registry <- package_registry("tsflowstartertest")
  expect_named(registry$transforms, "scale_series")
  expect_length(registry$hooks, 0L)
  expect_identical(environment(registry$transforms$scale_series), asNamespace("tsflowstartertest"))
  # Installed tsflow uses base::system.file(), not the shim load_all() injects.
  installed_lookup <- package_registry
  lookup_env <- new.env(parent = environment(package_registry))
  lookup_env$system.file <- base::system.file
  environment(installed_lookup) <- lookup_env
  expect_identical(installed_lookup("tsflowstartertest"), registry)
  expect_false("scale_series" %in% getNamespaceExports("tsflowstartertest"))
  run <- getExportedValue("tsflowstartertest", "run_pipeline")
  expect_equal(as.numeric(run(progress = FALSE)$values$adjusted), c(20, 40, 60, 80))
  results <- testthat::test_local(path, reporter = "silent")
  expect_true(all(vapply(results, function(test) all(vapply(test$results, function(result) {
    inherits(result, "expectation_success")
  }, logical(1))), logical(1))))
  expect_true(file.exists(file.path(path, "README.md")))
  expect_true(file.exists(file.path(path, "man", "run_pipeline.Rd")))
})

test_that("package targets use the same pipeline and can be enabled later", {
  skip_if_not_installed("targets")
  path <- local_scaffold_package()
  use_tsflow(path, targets = TRUE)
  expect_true(file.exists(file.path(path, "_targets.R")))
  withr::local_dir(path)
  targets::tar_make(callr_function = NULL, reporter = "silent")
  expect_equal(targets::tar_read(adjusted),
               getExportedValue("tsflowstartertest", "run_pipeline")(progress = FALSE)$values$adjusted)
  expect_length(targets::tar_outdated(callr_function = NULL), 0L)
})

test_that("transform and hook helpers produce functions usable by the registry", {
  path <- local_scaffold_package()
  use_tsflow_transform("new_transform", path)
  use_tsflow_hook("postprocess", path)
  use_tsflow_hook("binary_op", path)
  roxygen2::roxygenise(path)
  registry <- package_registry("tsflowstartertest")
  expect_setequal(names(registry$transforms), c("new_transform", "scale_series"))
  expect_named(registry$hooks, c("binary_op", "postprocess"))
  result <- getExportedValue("tsflowstartertest", "run_pipeline")(progress = FALSE)
  expect_equal(as.numeric(result$values$adjusted), c(20, 40, 60, 80))
  results <- testthat::test_local(path, reporter = "silent")
  expect_true(all(vapply(results, function(test) all(vapply(test$results, function(result) {
    inherits(result, "expectation_success")
  }, logical(1))), logical(1))))
  registry_path <- file.path(path, "inst", "tsflow", "registry.yml")
  registry_text <- yaml::read_yaml(registry_path)
  registry_text$transforms$new_transform <- "missing_function"
  yaml::write_yaml(registry_text, registry_path)
  expect_error(package_registry("tsflowstartertest"), "stale tsflow registry")
})

test_that("setup preserves existing package metadata and documentation settings", {
  skip_if_not_installed("roxygen2")
  dir <- withr::local_tempdir()
  write.dcf(data.frame(Package = "tsflowexistingtest", Version = "0.0.1",
                      Title = "Existing Package", Description = "An existing test package.",
                      Imports = "stats", Suggests = "testthat (>= 3.0.0)",
                      Roxygen = 'list(markdown = TRUE, roclets = c("rd", "namespace"))'),
            file.path(dir, "DESCRIPTION"))
  withr::defer(pkgload::unload("tsflowexistingtest"))
  writeLines("# Existing project notes", file.path(dir, "README.md"))
  use_tsflow(dir)
  expect_identical(readLines(file.path(dir, "README.md")), "# Existing project notes")
  expect_true(file.exists(file.path(dir, "TSFLOW.md")))
  description <- read.dcf(file.path(dir, "DESCRIPTION"))
  expect_match(description[1L, "Imports"], "stats, tsflow", fixed = TRUE)
  expect_match(description[1L, "Suggests"], "testthat (>= 3.0.0)", fixed = TRUE)
  config <- roxygen2::load_options(dir)
  expect_true(config$markdown)
  expect_equal(config$roclets, c("rd", "namespace", "tsflow::registry_roclet"))
})

test_that("invalid requests and existing stub files are rejected before writing", {
  expect_error(use_tsflow(targets = NA), "must be TRUE or FALSE")
  expect_error(use_tsflow(path = NA_character_), "single directory path")
  path <- local_scaffold_package()
  expect_error(create_tsflow_package(path), "already exists")
  expect_error(use_tsflow_transform("../outside", path), "syntactic R function name")
  expect_error(use_tsflow_hook("unknown", path), "must be.*binary_op")
  use_tsflow_transform("kept", path)
  stub <- file.path(path, "R", "kept.R")
  writeLines("# User code", stub)
  expect_error(use_tsflow_transform("kept", path), "refusing to overwrite")
  expect_identical(readLines(stub), "# User code")
  expect_error(use_tsflow_transform("foo", withr::local_tempdir()), "DESCRIPTION")
})
