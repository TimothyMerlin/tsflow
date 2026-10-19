combine <- function(a, b, op = "add") {
  spec <- normalize_pipeline(list(series = list(
    list(name = "a", source = list(type = "injected")),
    list(name = "b", source = list(type = "injected")),
    list(name = "out", steps = list(list(input = "a"), list(op = list(type = op, right = "b"))))
  )))
  evaluate_pipeline(spec, inputs = list(a = a, b = b), progress = FALSE)$values$out
}

quarterly <- function(start, n) ts(seq_len(n), start = start, frequency = 4)

test_that("ts series over the same period combine element-wise", {
  out <- combine(quarterly(c(2020, 1), 8), quarterly(c(2020, 1), 8))
  expect_equal(as.numeric(out), seq(2, 16, by = 2))
  expect_equal(stats::tsp(out), stats::tsp(quarterly(c(2020, 1), 8)))
})

test_that("partly overlapping ts series combine on their common period", {
  out <- combine(quarterly(c(2020, 1), 8), quarterly(c(2021, 1), 8))
  expect_equal(stats::start(out), c(2021, 1))
  expect_equal(stats::end(out), c(2021, 4))
  # 2021 values: a = 5..8, b = 1..4
  expect_equal(as.numeric(out), c(6, 8, 10, 12))
})

test_that("non-overlapping ts series give a clear error", {
  expect_error(
    combine(quarterly(c(2020, 1), 4), quarterly(c(2022, 1), 4)),
    "cannot add `a` and `b`: their periods don't overlap (2020Q1 to 2020Q4 and 2022Q1 to 2022Q4)",
    fixed = TRUE
  )
})

test_that("ts series with different frequencies give a clear error", {
  expect_error(
    combine(quarterly(c(2020, 1), 8), ts(1:2, start = 2020, frequency = 1), op = "div"),
    "cannot divide `a` and `b`: they have different frequencies (4 and 1)",
    fixed = TRUE
  )
})

test_that("mixing ts and xts gives a clear error instead of ignoring dates", {
  skip_if_not_installed("xts")
  x <- xts::xts(1:8, order.by = seq(as.Date("2020-01-01"), by = "quarter", length.out = 8))
  expect_error(
    combine(quarterly(c(2020, 1), 8), x),
    "cannot add `a` and `b`: one is a `ts` and the other an `xts`; convert the inputs to one class",
    fixed = TRUE
  )
})

test_that("xts series combine on their common dates and fail clearly without overlap", {
  skip_if_not_installed("xts")
  x <- function(from, n) xts::xts(seq_len(n), order.by = seq(as.Date(from), by = "quarter", length.out = n))

  out <- combine(x("2020-01-01", 8), x("2021-01-01", 8), op = "mul")
  expect_equal(NROW(out), 4L)
  expect_equal(format(min(zoo::index(out))), "2021-01-01")

  expect_error(
    combine(x("2020-01-01", 4), x("2022-01-01", 4)),
    "their periods don't overlap (2020-01-01 to 2020-10-01 and 2022-01-01 to 2022-10-01)",
    fixed = TRUE
  )
})

test_that("series combined with plain numbers are not checked", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "a", source = list(type = "injected")),
    list(name = "out", expr = "a * 100")
  )))
  out <- evaluate_pipeline(spec, inputs = list(a = quarterly(c(2020, 1), 4)), progress = FALSE)$values$out
  expect_equal(as.numeric(out), c(100, 200, 300, 400))
})

test_that("alignment errors name the series being evaluated and appear in step replay", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "a", source = list(type = "injected")),
    list(name = "b", source = list(type = "injected")),
    list(name = "out", expr = "a - b")
  )))
  inputs <- list(a = quarterly(c(2020, 1), 4), b = quarterly(c(2022, 1), 4))

  expect_error(
    evaluate_pipeline(spec, inputs = inputs, progress = FALSE),
    "failed while evaluating series `out`: cannot subtract `a` and `b`",
    fixed = TRUE
  )

  result <- suppressWarnings(evaluate_pipeline(spec, inputs = inputs, progress = FALSE, error_mode = "collect"))
  expanded <- expand_node_result(result, "out")
  expect_match(expanded$errors[["out::step_1"]], "cannot subtract `a` and `b`", fixed = TRUE)
})

test_that("a binary_op hook takes over alignment", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "a", source = list(type = "injected")),
    list(name = "b", source = list(type = "injected")),
    list(name = "out", expr = "a + b")
  )))
  result <- evaluate_pipeline(
    spec,
    inputs = list(a = quarterly(c(2020, 1), 4), b = quarterly(c(2022, 1), 4)),
    hooks = list(binary_op = function(op, left, right) "hook handled it"),
    progress = FALSE
  )
  expect_equal(result$values$out, "hook handled it")
})
