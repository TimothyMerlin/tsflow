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
