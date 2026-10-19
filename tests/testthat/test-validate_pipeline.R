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

test_that("validate_pipeline hints at literals for unknown refs in step args", {
  jsa <- function(x, method) x

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "jsa", kwargs = list(method = "uniform")))
        )
      )
    )
  ))

  err <- tryCatch(validate_pipeline(spec, transforms = local_transforms()), error = identity)
  expect_match(conditionMessage(err), "references unknown series: uniform", fixed = TRUE)
  expect_match(conditionMessage(err), "use `{literal: uniform}`", fixed = TRUE)
})

test_that("validate_pipeline hints at literals for unknown op operands", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(op = list(type = "div", right = "missing_series"))
        )
      )
    )
  ))

  expect_error(validate_pipeline(spec), "{literal: missing_series}", fixed = TRUE)
})

test_that("validate_pipeline gives no literal hint for an unknown input", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(name = "derived_b", steps = list(list(input = "missing_input")))
    )
  ))

  err <- tryCatch(validate_pipeline(spec), error = identity)
  expect_match(conditionMessage(err), "references unknown series: missing_input", fixed = TRUE)
  expect_no_match(conditionMessage(err), "literal", fixed = TRUE)
})

test_that("validate_pipeline rejects series names that are not valid R names", {
  bad_names <- c("gdp-real", "gdp real", "2020_gdp", ".hidden", "_x", "gdp..real", "TRUE")
  suggested <- c("gdp.real", "gdp.real", "X2020_gdp", "X.hidden", "X_x", "gdp.real", "TRUE.")

  for (idx in seq_along(bad_names)) {
    spec <- normalize_pipeline(list(series = list(
      list(name = bad_names[[idx]], source = list(type = "injected"))
    )))

    err <- tryCatch(validate_pipeline(spec), error = identity)
    expect_s3_class(err, "error")
    expect_match(
      conditionMessage(err),
      paste("series names must be valid R names:", bad_names[[idx]]),
      fixed = TRUE
    )
    expect_match(conditionMessage(err), paste("Suggested:", suggested[[idx]]), fixed = TRUE)
  }
})

test_that("validate_pipeline lists all invalid series names at once", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "gdp-real", source = list(type = "injected")),
    list(name = "ok.name", source = list(type = "injected")),
    list(name = "2020_gdp", steps = list(list(input = "ok.name")))
  )))

  expect_error(
    validate_pipeline(spec),
    "series names must be valid R names: gdp-real, 2020_gdp",
    fixed = TRUE
  )
})

test_that("validate_pipeline accepts dotted and underscored series names", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "gdp.nominal.quarterly", source = list(type = "injected")),
    list(name = "gdp_real2", steps = list(list(input = "gdp.nominal.quarterly")))
  )))

  expect_invisible(validate_pipeline(spec))
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

  expect_invisible(validate_pipeline(spec, transforms = local_transforms()))
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
    validate_pipeline(spec, transforms = list()),
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
