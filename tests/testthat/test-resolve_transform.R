test_that("resolve_transform supports namespaced and bare function names", {
  local_scale <- function(x, factor) x * factor

  fn1 <- resolve_transform("stats::lag")
  fn2 <- resolve_transform("local_scale", transform_env = environment())

  expect_true(is.function(fn1))
  expect_true(is.function(fn2))
  expect_equal(fn2(2, 3), 6)
})

test_that("resolve_transform fails clearly for invalid names", {
  expect_error(
    resolve_transform(1),
    "transform name must be a single string"
  )

  expect_error(
    resolve_transform("missing_fn", transform_env = baseenv()),
    "transform `missing_fn` could not be resolved"
  )

  expect_error(
    resolve_transform("pkg::"),
    "invalid namespaced transform"
  )
})
