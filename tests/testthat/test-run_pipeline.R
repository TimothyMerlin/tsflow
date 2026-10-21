test_that("execute_pipeline executes the full YAML-to-result flow", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10",
      "outputs:",
      "  - derived_b"
    ),
    con = path
  )

  result <- execute_pipeline(
    path = path,
    inputs = list(
      raw_a = ts(5, start = c(2024, 1), frequency = 12)
    ),
    transforms = local_transforms()
  )

  expect_s3_class(result, "PipelineResult")
  expect_equal(as.numeric(result$outputs$derived_b), 50)
  expect_equal(result$plan$order, c("raw_a", "derived_b"))
})

test_that("execute_pipeline lets explicit outputs override YAML outputs", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10"
    ),
    con = path
  )

  result <- execute_pipeline(
    path = path,
    inputs = list(
      raw_a = ts(5, start = c(2024, 1), frequency = 12)
    ),
    outputs = "raw_a",
    transforms = local_transforms()
  )

  expect_equal(names(result$outputs), "raw_a")
  expect_true(stats::is.ts(result$outputs$raw_a))
})

test_that("execute_pipeline defaults to derived outputs and respects the progress option", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10"
    ),
    con = path
  )

  old <- getOption("tsflow.progress")
  options(tsflow.progress = FALSE)
  on.exit(options(tsflow.progress = old), add = TRUE)

  result <- execute_pipeline(
    path = path,
    inputs = list(raw_a = ts(5, start = c(2024, 1), frequency = 12)),
    transforms = local_transforms()
  )

  expect_equal(names(result$outputs), "derived_b")
  expect_equal(as.numeric(result$outputs$derived_b), 50)
  expect_equal(names(result$values), c("raw_a", "derived_b"))
})

test_that("execute_pipeline surfaces normalization failures and missing inputs", {
  bad_path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: bad_a"
    ),
    con = bad_path
  )

  expect_error(
    execute_pipeline(
      path = bad_path,
      inputs = list()
    ),
    "must define either `source`, `steps` or `expr`"
  )

  missing_input_path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected"
    ),
    con = missing_input_path
  )

  expect_warning(
    execute_pipeline(
      path = missing_input_path,
      inputs = list()
    ),
    "missing: raw_a"
  )
})

test_that("run_pipeline is removed", {
  expect_false(exists("run_pipeline", envir = asNamespace("tsflow"), inherits = FALSE))
})

test_that("execute_pipeline exposes collect mode through the file wrapper", {
  boom <- function(x) stop("kaboom", call. = FALSE)
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: raw_b",
      "    source:",
      "      type: injected",
      "  - name: left_fail",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: boom",
      "  - name: right_ok",
      "    steps:",
      "      - input: raw_b",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10",
      "outputs:",
      "  - left_fail",
      "  - right_ok"
    ),
    con = path
  )

  result <- NULL
  expect_warning(
    result <- execute_pipeline(
      path = path,
      inputs = list(
        raw_a = ts(1, start = c(2024, 1), frequency = 12),
        raw_b = ts(5, start = c(2024, 1), frequency = 12)
      ),
      transforms = local_transforms(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expect_equal(result$error_mode, "collect")
  expect_equal(names(result$errors), "left_fail")
  expect_match(result$errors$left_fail, "transform `boom` failed: kaboom")
  expect_equal(names(result$outputs), "right_ok")
  expect_equal(as.numeric(result$outputs$right_ok), 50)
})

test_that("execute_pipeline runs the packaged CSV example end to end", {
  skip_if_not_installed("tempdisagg")
  skip_if_not_installed("zoo")

  example_path <- system.file("examples", "gdp_construction.csv", package = "tsflow")
  data("gdp_construction_inputs", package = "tsflow")

  transforms_env <- new.env()
  sys.source(
    system.file("examples", "gdp_construction_transforms.R", package = "tsflow"),
    envir = transforms_env
  )

  result <- execute_pipeline(
    path = example_path,
    inputs = gdp_construction_inputs,
    transforms = local_transforms(transforms_env)
  )

  expect_s3_class(result, "PipelineResult")
  expect_true(all(c(
    "gdp.nominal.quarterly",
    "construction.real.quarterly",
    "construction.deflator.quarterly"
  ) %in% names(result$outputs)))
  expect_true(stats::is.ts(result$outputs$gdp.nominal.quarterly))
  expect_true(stats::is.ts(result$outputs$construction.real.quarterly))
  expect_true(stats::is.ts(result$outputs$construction.deflator.quarterly))
  expect_true(length(result$plan$order) >= 3)
})
