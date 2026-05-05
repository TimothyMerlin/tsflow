test_that("extract_dependencies walks the normalized expression tree", {
  expr <- normalize_steps(list(
    list(input = "raw_a"),
    list(call = list(
      name = "join",
      args = list("raw_b", 100),
      kwargs = list(label = list(literal = "idx"), ref = list(ref = "raw_c"))
    )),
    list(op = list(
      type = "div",
      right = "raw_d"
    ))
  ))

  expect_equal(
    extract_dependencies(expr),
    c("raw_a", "raw_b", "raw_c", "raw_d")
  )
})

test_that("extract_dependencies rejects unsupported expression objects", {
  expect_error(
    extract_dependencies(list(bad = TRUE)),
    "unsupported expression class"
  )
})

test_that("collect_transform_names traverses normalized expressions", {
  expr <- normalize_steps(list(
    list(input = "raw_a"),
    list(call = list(name = "scale", args = list(100))),
    list(call = list(name = "join", args = list("raw_b")))
  ))

  expect_equal(collect_transform_names(expr), c("scale", "join"))
})
