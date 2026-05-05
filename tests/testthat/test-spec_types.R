test_that("spec constructors build expected classes and fields", {
  source <- new_source_spec("file", list(path = "data.csv"))
  ref <- new_ref_node("raw_a")
  literal <- new_literal_node(100)
  call <- new_call_node("scale", args = list(literal), kwargs = list(reference = ref))
  pipe <- new_pipe_node(ref, call)
  expr <- new_binary_op_node("div", pipe, literal)
  series <- new_series_spec(
    name = "derived_a",
    kind = "derived",
    source = NULL,
    expression = expr,
    description = "desc",
    metadata = list(owner = "kof")
  )
  pipeline <- new_pipeline_spec(list(series), outputs = "derived_a", metadata = list(version = 1))

  expect_s3_class(source, "SourceSpec")
  expect_equal(source$type, "file")
  expect_equal(source$config$path, "data.csv")

  expect_s3_class(ref, "RefNode")
  expect_equal(ref$name, "raw_a")

  expect_s3_class(literal, "LiteralNode")
  expect_equal(literal$value, 100)

  expect_s3_class(call, "CallNode")
  expect_equal(call$name, "scale")
  expect_identical(call$args[[1]], literal)
  expect_identical(call$kwargs$reference, ref)

  expect_s3_class(pipe, "PipeNode")
  expect_identical(pipe$input, ref)
  expect_identical(pipe$call, call)

  expect_s3_class(expr, "BinaryOpNode")
  expect_equal(expr$op, "div")
  expect_identical(expr$left, pipe)
  expect_identical(expr$right, literal)

  expect_s3_class(series, "SeriesSpec")
  expect_equal(series$name, "derived_a")
  expect_equal(series$kind, "derived")
  expect_identical(series$expression, expr)
  expect_equal(series$description, "desc")
  expect_equal(series$metadata$owner, "kof")

  expect_s3_class(pipeline, "PipelineSpec")
  expect_identical(pipeline$series[[1]], series)
  expect_equal(pipeline$outputs, "derived_a")
  expect_equal(pipeline$metadata$version, 1)
})
