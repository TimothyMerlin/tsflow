write_cached_pipeline <- function(path) {
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source: {type: injected}",
      "  - name: raw_b",
      "    source: {type: injected}",
      "  - name: scaled",
      "    expr: \"scale(raw_a, factor = 10)\"",
      "  - name: grown",
      "    expr: \"growth(scaled) + raw_b\"",
      "  - name: other",
      "    expr: \"scale(raw_b, factor = 2)\""
    ),
    path
  )
}

test_that("execute_pipeline(store) caches and only reruns what changed", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  store <- file.path(dir, "cache")
  write_cached_pipeline(path)

  scale <- function(x, factor) x * factor
  growth <- function(x) helper(x) * 2
  helper <- function(x) x + 1
  inputs <- list(raw_a = ts(c(1, 2, 3)), raw_b = ts(c(5, 5, 5)))
  run <- function() {
    execute_pipeline(path, inputs, transforms = list(scale = scale, growth = growth), progress = FALSE, store = store)
  }

  first <- run()
  direct <- execute_pipeline(path, inputs, transforms = list(scale = scale, growth = growth), progress = FALSE)
  expect_equal(first$values, direct$values[names(first$values)])
  expect_equal(first$outputs, direct$outputs)
  expect_equal(first$input_hashes, direct$input_hashes)
  expect_setequal(first$rebuilt, c("scaled", "grown", "other"))
  expect_s3_class(first, "PipelineResult")

  expect_length(run()$rebuilt, 0L)

  growth <- function(x) helper(x) * 3
  expect_equal(run()$rebuilt, "grown")

  helper <- function(x) x + 100
  expect_equal(run()$rebuilt, "grown")

  inputs$raw_b <- ts(c(6, 6, 6))
  expect_setequal(run()$rebuilt, c("grown", "other"))
})

test_that("tsflow.store option turns caching on and store = NULL turns it off", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  write_cached_pipeline(path)
  withr::local_options(tsflow.store = file.path(dir, "cache"))

  scale <- function(x, factor) x * factor
  growth <- function(x) x + 1
  inputs <- list(raw_a = ts(1:3), raw_b = ts(1:3))
  transforms <- list(scale = scale, growth = growth)

  cached <- execute_pipeline(path, inputs, transforms = transforms, progress = FALSE)
  expect_false(is.null(cached$rebuilt))
  expect_true(dir.exists(cached$store))
  expect_match(basename(cached$store), "^p-[0-9a-f]{6}$")

  in_memory <- execute_pipeline(path, inputs, transforms = transforms, progress = FALSE, store = NULL)
  expect_null(in_memory$rebuilt)
})

test_that("pipelines sharing a store and series names use separate subfolders", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  store <- file.path(dir, "cache")
  dir.create(file.path(dir, "one"))
  dir.create(file.path(dir, "two"))
  for (sub in c("one", "two")) {
    writeLines(
      c(
        "series:",
        "  - name: raw_a",
        "    source: {type: injected}",
        "  - name: gdp",
        sprintf("    expr: \"scale(raw_a, factor = %d)\"", if (sub == "one") 2L else 3L)
      ),
      file.path(dir, sub, "p.yml")
    )
  }

  scale <- function(x, factor) x * factor
  inputs <- list(raw_a = ts(1:3))
  one <- execute_pipeline(file.path(dir, "one", "p.yml"), inputs, transforms = list(scale = scale), progress = FALSE, store = store)
  two <- execute_pipeline(file.path(dir, "two", "p.yml"), inputs, transforms = list(scale = scale), progress = FALSE, store = store)
  one_again <- execute_pipeline(file.path(dir, "one", "p.yml"), inputs, transforms = list(scale = scale), progress = FALSE, store = store)

  expect_false(identical(one$store, two$store))
  expect_equal(as.numeric(one_again$values$gdp), c(2, 4, 6))
  expect_equal(as.numeric(two$values$gdp), c(3, 6, 9))
  expect_length(one_again$rebuilt, 0L)
})

test_that("cached execution reports failures like in-memory execution", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  store <- file.path(dir, "cache")
  write_cached_pipeline(path)

  scale <- function(x, factor) x * factor
  growth <- function(x) stop("kaboom", call. = FALSE)
  inputs <- list(raw_a = ts(1:3), raw_b = ts(1:3))
  transforms <- list(scale = scale, growth = growth)

  expect_error(
    execute_pipeline(path, inputs, transforms = transforms, progress = FALSE, store = store),
    "failed while evaluating series `grown`: transform `growth` failed: kaboom",
    fixed = TRUE
  )

  expect_warning(
    collected <- execute_pipeline(path, inputs, transforms = transforms, progress = FALSE, store = store, error_mode = "collect"),
    "failed: grown"
  )
  expect_equal(names(collected$errors), "grown")
  expect_match(collected$errors$grown, "kaboom")
  expect_setequal(names(collected$values), c("raw_a", "raw_b", "scaled", "other"))
  expect_equal(collected$dag$node_status[["grown"]], "failed")
})

test_that("cached execution skips series downstream of a failure", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source: {type: injected}",
      "  - name: broken",
      "    expr: \"explode(raw_a)\"",
      "  - name: after",
      "    expr: \"broken + 1\""
    ),
    path
  )

  explode <- function(x) stop("kaboom", call. = FALSE)
  result <- suppressWarnings(execute_pipeline(
    path,
    list(raw_a = ts(1:3)),
    transforms = list(explode = explode),
    progress = FALSE,
    store = file.path(dir, "cache"),
    error_mode = "collect"
  ))

  expect_equal(names(result$errors), "broken")
  expect_equal(result$skipped, "after")
  expect_null(result$values$after)
})

test_that("cached execution leaves the targets error option unchanged", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  write_cached_pipeline(path)
  before <- targets::tar_option_get("error")

  scale <- function(x, factor) x * factor
  growth <- function(x) x + 1
  suppressWarnings(execute_pipeline(
    path,
    list(raw_a = ts(1:3), raw_b = ts(1:3)),
    transforms = list(scale = scale, growth = growth),
    progress = FALSE,
    store = file.path(dir, "cache"),
    error_mode = "collect"
  ))

  expect_identical(targets::tar_option_get("error"), before)
})

test_that("cached execution validates before running", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- file.path(dir, "p.yml")
  write_cached_pipeline(path)

  expect_error(
    execute_pipeline(path, list(), progress = FALSE, store = file.path(dir, "cache")),
    "transform `scale` is not registered"
  )
  expect_error(
    execute_pipeline(path, list(), progress = FALSE, store = c("a", "b")),
    "`store` must be a single directory path"
  )
  expect_false(dir.exists(file.path(dir, "cache")))
})
