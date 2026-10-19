test_that("run_pipeline executes the full YAML-to-result flow", {
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

  result <- run_pipeline(
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

test_that("run_pipeline lets explicit outputs override YAML outputs", {
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

  result <- run_pipeline(
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

test_that("run_pipeline surfaces normalization and execution failures", {
  bad_path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: bad_a"
    ),
    con = bad_path
  )

  expect_error(
    run_pipeline(
      path = bad_path,
      inputs = list()
    ),
    "must define either `source` or `steps`"
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

  expect_error(
    run_pipeline(
      path = missing_input_path,
      inputs = list()
    ),
    "raw input `raw_a` is not available in `inputs`"
  )
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

  jsa <- function(
    ts_sa,
    ts_nsa,
    method = "uniform",
    conversion = "sum"
  ) {
    ts_nsa_a <- tempdisagg::ta(ts_nsa, conversion = conversion, to = "annual")

    if (method == "denton-cholette") {
      return(tempdisagg::td(
        ts_nsa_a ~ 0 + ts_sa,
        method = "denton-cholette",
        criterion = "additive",
        h = 1
      )$values)
    }

    tempdisagg::td(
      ts_nsa_a ~ ts_sa,
      conversion = conversion,
      to = "quarterly",
      method = method
    )$values
  }

  real_gr_nom <- function(ts_gr, ts_nom, base_year) {
    ts_gr <- zoo::na.trim(ts_gr)
    if (frequency(ts_gr) < frequency(ts_nom)) {
      ts_nom <- tempdisagg::ta(
        ts_nom,
        conversion = "sum",
        to = frequency(ts_gr)
      )
    }

    ts_real <- ts(
      NA,
      start = start(ts_gr) - c(1, 0),
      end = end(ts_gr),
      frequency = frequency(ts_gr)
    )

    base_year_value <- as.numeric(window(
      ts_nom,
      start = c(base_year, 1),
      end = c(base_year, 1)
    ))
    window(ts_real, start = c(base_year, 1), end = c(base_year, 1)) <- base_year_value

    product_up <- (ts_gr / 100 + 1)
    if (base_year < end(ts_gr)[1]) {
      up_vals <- as.numeric(window(product_up, start = c(base_year + 1, 1)))
      window(ts_real, start = c(base_year + 1, 1)) <- base_year_value * cumprod(up_vals)
    }

    product_down <- 1 / product_up
    if (base_year > start(ts_gr)[1]) {
      down_vals <- as.numeric(window(product_down, end = c(base_year, 1)))
      window(ts_real, end = c(base_year - 1, 1)) <- rev(base_year_value * cumprod(rev(down_vals)))
    }

    ts_real
  }

  a2q_sum <- function(ts_d, indicator, conversion = "sum") {
    ts_d <- zoo::na.trim(ts_d)
    indicator <- zoo::na.trim(indicator)

    start_td <- max(stats::time(indicator)[1], stats::time(ts_d)[1])
    end_td <- utils::tail(stats::time(indicator), 1)

    indicator <- window(indicator, start = start_td, end = end_td)

    if (any(!is.na(ts_d)) && any(!is.na(indicator))) {
      return(tempdisagg::td(
        ts_d ~ indicator,
        conversion = conversion,
        to = "quarterly",
        method = "chow-lin-maxlog"
      )$values)
    }

    tempdisagg::td(
      ts_d ~ 1,
      conversion = conversion,
      to = "quarterly",
      method = "denton-cholette"
    )$values
  }

  result <- execute_pipeline(
    path = example_path,
    inputs = gdp_construction_inputs,
    transforms = local_transforms()
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
