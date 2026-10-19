test_that("null coalescing helper returns the first non-null value", {
  expect_equal(`%||%`(NULL, "fallback"), "fallback")
  expect_equal(`%||%`("value", "fallback"), "value")
})

test_that("scalar helper predicates classify supported inputs", {
  expect_true(is_scalar_string("x"))
  expect_false(is_scalar_string(c("x", "y")))
  expect_false(is_scalar_string(1))

  expect_true(is_scalar_number(1))
  expect_true(is_scalar_number(1.5))
  expect_false(is_scalar_number(c(1, 2)))
  expect_false(is_scalar_number("1"))

  expect_true(is_scalar_flag(TRUE))
  expect_false(is_scalar_flag(c(TRUE, FALSE)))
  expect_false(is_scalar_flag(1))

  expect_true(is_scalar_value(NULL))
  expect_true(is_scalar_value("x"))
  expect_true(is_scalar_value(1))
  expect_true(is_scalar_value(FALSE))
  expect_false(is_scalar_value(list("x")))

  expect_true(is_literal_value(c(2026, 4)))
  expect_true(is_literal_value(c("a", "b")))
  expect_false(is_literal_value(list("x")))
  expect_false(is_literal_value(c(1, NA)))
})

test_that("assert helpers fail with targeted messages", {
  expect_error(assert_list("x", "thing"), "thing must be a list")
  expect_error(assert_named_list(list("x"), "thing"), "thing must be a named list")
  expect_error(
    assert_inherits(list(), "PipelineSpec", "pipeline"),
    "pipeline must inherit from `PipelineSpec`"
  )
})

test_that("fail aborts without a call", {
  err <- tryCatch(fail("boom"), error = identity)
  expect_s3_class(err, "error")
  expect_equal(conditionMessage(err), "boom")
  expect_null(err$call)
})

test_that("fail and warn print braces literally instead of evaluating them", {
  err <- tryCatch(fail("unknown series: {Sys.getenv('HOME')}"), error = identity)
  expect_equal(conditionMessage(err), "unknown series: {Sys.getenv('HOME')}")

  err <- tryCatch(fail("unknown series: {a"), error = identity)
  expect_equal(conditionMessage(err), "unknown series: {a")

  expect_warning(warn("value }{ here"), "value }{ here", fixed = TRUE)
})

test_that("fail keeps cli bullet names", {
  err <- tryCatch(fail(c("header", "x" = "failed: {b}")), error = identity)
  expect_match(conditionMessage(err), "failed: {b}", fixed = TRUE)
})

test_that("validation errors show user-supplied names verbatim", {
  pipeline <- normalize_pipeline(list(series = list(
    list(name = "a", source = list(type = "injected")),
    list(name = "b", steps = list(list(input = "{Sys.getenv('HOME')}")))
  )))

  expect_error(
    validate_pipeline(pipeline),
    "references unknown series: {Sys.getenv('HOME')}",
    fixed = TRUE
  )
})
