test_that("normalize_pipeline builds canonical raw and derived specs", {
  parsed <- list(
    series = list(
      list(
        name = "raw_a",
        source = list(
          type = "internal_api",
          key = "raw_a"
        )
      ),
      list(
        name = "derived_b",
        description = "example",
        steps = list(
          list(input = "raw_a"),
          list(call = list(
            name = "scale",
            args = list(100),
            kwargs = list(reference = "raw_a", label = list(literal = "idx"))
          )),
          list(op = list(
            type = "div",
            right = 2
          ))
        )
      )
    ),
    outputs = c("derived_b"),
    metadata = list(owner = "kof")
  )

  spec <- normalize_pipeline(parsed)

  expect_s3_class(spec, "PipelineSpec")
  expect_length(spec$series, 2)
  expect_equal(spec$outputs, "derived_b")
  expect_equal(spec$metadata$owner, "kof")

  raw_series <- spec$series[[1]]
  expect_s3_class(raw_series, "SeriesSpec")
  expect_equal(raw_series$kind, "raw")
  expect_equal(raw_series$source$type, "internal_api")
  expect_equal(raw_series$source$config$key, "raw_a")
  expect_null(raw_series$expression)

  derived_series <- spec$series[[2]]
  expect_equal(derived_series$kind, "derived")
  expect_s3_class(derived_series$expression, "BinaryOpNode")
  expect_equal(derived_series$expression$op, "div")
  expect_s3_class(derived_series$expression$left, "PipeNode")
  expect_equal(derived_series$expression$left$call$name, "scale")
  expect_s3_class(derived_series$expression$left$call$args[[1]], "LiteralNode")
  expect_equal(derived_series$expression$left$call$args[[1]]$value, 100)
  expect_s3_class(derived_series$expression$left$call$kwargs$reference, "RefNode")
  expect_equal(derived_series$expression$left$call$kwargs$reference$name, "raw_a")
  expect_s3_class(derived_series$expression$left$call$kwargs$label, "LiteralNode")
  expect_equal(derived_series$expression$left$call$kwargs$label$value, "idx")
  expect_s3_class(derived_series$expression$right, "LiteralNode")
  expect_equal(derived_series$expression$right$value, 2)
})

test_that("normalize_series rejects invalid shapes", {
  expect_error(
    normalize_series(list(name = "bad")),
    "must define either `source` or `steps`"
  )

  expect_error(
    normalize_series(list(
      name = "bad",
      source = list(type = "file"),
      steps = list(list(input = "x"))
    )),
    "cannot define both `source` and `steps`"
  )
})

test_that("normalize_steps enforces a single first input step", {
  expect_error(
    normalize_steps(list(
      list(call = list(name = "scale"))
    )),
    "exactly one `input` step"
  )

  expect_error(
    normalize_steps(list(
      list(call = list(name = "scale")),
      list(input = "raw_a")
    )),
    "the `input` step must be first"
  )

  expect_error(
    normalize_steps(list(
      list(input = "raw_a"),
      list(input = "raw_b")
    )),
    "exactly one `input` step"
  )
})

test_that("normalize_step_arg rejects unsupported values", {
  expect_error(
    normalize_step_arg(list(a = 1, b = 2)),
    "literal value, series reference, var or nested call"
  )
})

test_that("normalize_pipeline validates top-level outputs and metadata", {
  expect_error(
    normalize_pipeline(list(series = list(list(name = "raw_a", source = list(type = "file"))), outputs = 1)),
    "pipeline `outputs` must be a character vector"
  )

  expect_error(
    normalize_pipeline(list(series = list(list(name = "raw_a", source = list(type = "file"))), metadata = "bad")),
    "pipeline `metadata` must be a map/list"
  )

  expect_error(
    normalize_pipeline(list(series = list(list(name = "raw_a", source = list(type = "file"))), vars = 1)),
    "pipeline `vars` must be a named list"
  )
})

test_that("normalize_source validates source shapes", {
  expect_error(normalize_source("bad"), "source must be a list")
  expect_error(normalize_source(list(path = "x.csv")), "source `type` must be a single string")

  source <- normalize_source(list(type = "file", path = "x.csv", column = "value"))
  expect_s3_class(source, "SourceSpec")
  expect_equal(source$type, "file")
  expect_equal(source$config$path, "x.csv")
  expect_equal(source$config$column, "value")
})

test_that("normalize_step_arg supports explicit literal and ref wrappers", {
  literal <- normalize_step_arg(list(literal = "label"))
  ref <- normalize_step_arg(list(ref = "upstream"))

  expect_s3_class(literal, "LiteralNode")
  expect_equal(literal$value, "label")
  expect_s3_class(ref, "RefNode")
  expect_equal(ref$name, "upstream")
})

test_that("normalize_step_arg supports vector literals", {
  literal <- normalize_step_arg(list(literal = c(2026, 4)))

  expect_s3_class(literal, "LiteralNode")
  expect_equal(literal$value, c(2026, 4))
})

test_that("normalize_step_arg resolves explicit var wrappers", {
  value <- normalize_step_arg(list(var = "base_year"), vars = list(base_year = 2020))

  expect_s3_class(value, "LiteralNode")
  expect_equal(value$value, 2020)

  expect_error(
    normalize_step_arg(list(var = "missing"), vars = list(base_year = 2020)),
    "unknown var `missing`"
  )
})

test_that("normalize_step_arg resolves vector vars", {
  value <- normalize_step_arg(list(var = "cutoff"), vars = list(cutoff = c(2026, 4)))

  expect_s3_class(value, "LiteralNode")
  expect_equal(value$value, c(2026, 4))
})

test_that("normalize_pipeline resolves vars in derived steps", {
  spec <- normalize_pipeline(list(
    vars = list(base_year = 2020, method = "denton-cholette"),
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(
            name = "foo",
            kwargs = list(base_year = list(var = "base_year"), method = list(var = "method"))
          ))
        )
      )
    )
  ))

  kwargs <- spec$series[[2]]$expression$call$kwargs

  expect_equal(spec$vars$base_year, 2020)
  expect_s3_class(kwargs$base_year, "LiteralNode")
  expect_equal(kwargs$base_year$value, 2020)
  expect_s3_class(kwargs$method, "LiteralNode")
  expect_equal(kwargs$method$value, "denton-cholette")
})

test_that("normalize_pipeline allows vector vars in derived steps", {
  spec <- normalize_pipeline(list(
    vars = list(cutoff = c(2026, 4)),
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(
            name = "foo",
            kwargs = list(end = list(var = "cutoff"))
          ))
        )
      )
    )
  ))

  kwargs <- spec$series[[2]]$expression$call$kwargs

  expect_equal(spec$vars$cutoff, c(2026, 4))
  expect_s3_class(kwargs$end, "LiteralNode")
  expect_equal(kwargs$end$value, c(2026, 4))
})

test_that("normalize_steps validates call and op step shapes", {
  expect_error(
    normalize_steps(list(
      list(input = "raw_a"),
      list(call = list(args = list(1)))
    )),
    "call step 2 `name` must be a single string"
  )

  expect_error(
    normalize_steps(list(
      list(input = "raw_a"),
      list(call = list(name = "scale", kwargs = list(1, 2)))
    )),
    "call step 2 `kwargs` must be a named list"
  )

  expect_error(
    normalize_steps(list(
      list(input = "raw_a"),
      list(op = list(type = "pow", right = 2))
    )),
    "op step 2 `type` must be one of add, sub, mul, div"
  )

  expect_error(
    normalize_steps(list(
      list(input = "raw_a"),
      list(op = list(type = "div"))
    )),
    "op step 2 must define `right`"
  )
})
