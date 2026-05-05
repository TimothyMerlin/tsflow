test_that("validate_pipeline rejects duplicate series names", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "dup", source = list(type = "file", path = "a.csv")),
      list(name = "dup", source = list(type = "file", path = "b.csv"))
    )
  ))

  expect_error(
    validate_pipeline(spec),
    "duplicate series names are not allowed"
  )
})

test_that("validate_pipeline rejects unknown dependencies", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "join", args = list("missing_series")))
        )
      )
    )
  ))

  expect_error(
    validate_pipeline(spec),
    "references unknown series: missing_series"
  )
})

test_that("validate_pipeline rejects unknown outputs", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv"))
    ),
    outputs = c("missing_output")
  ))

  expect_error(
    validate_pipeline(spec),
    "outputs` reference unknown series"
  )
})

test_that("validate_pipeline resolves transform names from R", {
  scale <- function(x, factor) x * factor

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      )
    )
  ))

  expect_invisible(validate_pipeline(spec, transform_env = environment()))
})

test_that("validate_pipeline resolves namespaced transform names", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "stats::lag", args = list(1)))
        )
      )
    )
  ))

  expect_invisible(validate_pipeline(spec))
})

test_that("validate_pipeline rejects unresolved transform names", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "missing_scale", args = list(100)))
        )
      )
    )
  ))

  expect_error(
    validate_pipeline(spec, transform_env = baseenv()),
    "uses unresolved transform `missing_scale`"
  )
})

test_that("validate_pipeline rejects unsupported series kinds", {
  bad_series <- structure(
    list(
      name = "bad_a",
      kind = "mystery",
      source = NULL,
      expression = NULL,
      description = NULL,
      metadata = NULL
    ),
    class = c("SeriesSpec", "tsflow_spec")
  )

  pipeline <- new_pipeline_spec(list(bad_series))

  expect_error(
    validate_pipeline(pipeline),
    "series `bad_a` has unsupported kind `mystery`"
  )
})
