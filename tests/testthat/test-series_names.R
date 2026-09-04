make_spec <- function() {
  normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory", key = "raw_a")),
      list(name = "raw_b", source = list(type = "memory", key = "raw_b")),
      list(
        name = "derived_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10)))
        )
      ),
      list(
        name = "derived_d",
        steps = list(
          list(input = "derived_c"),
          list(op = list(type = "div", right = 2))
        )
      )
    ),
    outputs = "derived_d"
  ))
}

test_that("series_names returns every name in definition order", {
  expect_equal(
    series_names(make_spec()),
    c("raw_a", "raw_b", "derived_c", "derived_d")
  )
})

test_that("series_names filters by kind", {
  spec <- make_spec()
  expect_equal(series_names(spec, kind = "raw"), c("raw_a", "raw_b"))
  expect_equal(series_names(spec, kind = "derived"), c("derived_c", "derived_d"))
  expect_equal(
    series_names(spec, kind = c("raw", "derived")),
    c("raw_a", "raw_b", "derived_c", "derived_d")
  )
})

test_that("series_names accepts a PipelinePlan", {
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

  plan <- plan_pipeline(path = path, transform_env = environment())

  expect_equal(series_names(plan), c("raw_a", "derived_b"))
  expect_equal(series_names(plan, kind = "derived"), "derived_b")
})

test_that("series_names accepts a PipelineResult", {
  scale <- function(x, factor) x * factor

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory", key = "raw_a")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10)))
        )
      )
    ),
    outputs = "derived_b"
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(5, start = c(2024, 1), frequency = 12)),
    transform_env = environment()
  )

  expect_s3_class(result, "PipelineResult")
  expect_equal(series_names(result), c("raw_a", "derived_b"))
  expect_equal(series_names(result, kind = "raw"), "raw_a")
})

test_that("series_names rejects objects that are not a pipeline", {
  expect_error(
    series_names(list(series = list(list(name = "raw_a")))),
    "must be a `PipelineSpec`"
  )
  expect_error(series_names(42), "must be a `PipelineSpec`")
})

test_that("series_names rejects an unknown or non-character kind", {
  spec <- make_spec()
  expect_error(series_names(spec, kind = "derivd"), "subset of")
  expect_error(series_names(spec, kind = 1), "subset of")
})

test_that("series_names fails when a spec defines no series", {
  empty <- new_pipeline_spec(series = list())
  expect_error(series_names(empty), "no `series`")
})
