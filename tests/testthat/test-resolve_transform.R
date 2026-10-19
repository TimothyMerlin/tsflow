test_that("resolve_transform supports namespaced and registered function names", {
  local_scale <- function(x, factor) x * factor

  fn1 <- resolve_transform("stats::lag")
  fn2 <- resolve_transform("local_scale", transforms = list(local_scale = local_scale))

  expect_true(is.function(fn1))
  expect_true(is.function(fn2))
  expect_equal(fn2(2, 3), 6)
})

test_that("resolve_transform does not fall back to the calling environment", {
  local_scale <- function(x, factor) x * factor

  expect_error(
    resolve_transform("local_scale"),
    "transform `local_scale` is not registered in `transforms`"
  )
  expect_error(
    resolve_transform("scale", transforms = list()),
    "register it or use a namespaced `pkg::fn` name"
  )
})

test_that("resolve_transform fails clearly for invalid names", {
  expect_error(
    resolve_transform(1),
    "transform name must be a single string"
  )

  expect_error(
    resolve_transform("missing_fn", transforms = list()),
    "transform `missing_fn` is not registered in `transforms`"
  )

  expect_error(
    resolve_transform("pkg::"),
    "invalid namespaced transform"
  )
})

test_that("resolve_transform validates the transforms registry", {
  expect_error(resolve_transform("f", transforms = environment()), "named list of functions")
  expect_error(resolve_transform("f", transforms = list(function(x) x)), "named list of functions")
  expect_error(resolve_transform("f", transforms = list(f = 1)), "entries must be functions: f")
  expect_error(
    resolve_transform("f", transforms = list(f = identity, f = identity)),
    "duplicate names: f"
  )
})

test_that("resolve_transform respects tsflow.allowed_packages", {
  withr::local_options(tsflow.allowed_packages = "stats")

  expect_true(is.function(resolve_transform("stats::lag")))
  expect_error(
    resolve_transform("utils::head"),
    "package `utils` is not in `options(tsflow.allowed_packages)`",
    fixed = TRUE
  )
})

test_that("hooks must be a named list of supported functions", {
  expect_error(new_runtime(hooks = list(identity)), "named list of functions")
  expect_error(new_runtime(hooks = list(other = identity)), "unsupported `hooks`: other")
  expect_error(new_runtime(hooks = list(binary_op = 1)), "entries must be functions: binary_op")
  expect_equal(new_runtime(hooks = list(binary_op = identity))$hooks$binary_op, identity)
})
