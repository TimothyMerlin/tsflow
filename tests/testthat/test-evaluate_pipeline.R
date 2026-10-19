test_that("evaluate_pipeline executes raw and derived nodes end to end", {
  scale <- function(x, factor) x * factor
  add_n <- function(x, n) x + n

  spec <- normalize_pipeline(list(
    series = list(
      list(
        name = "raw_a",
        source = list(type = "memory", key = "raw_a")
      ),
      list(
        name = "raw_b",
        source = list(type = "memory", key = "raw_b")
      ),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "add_n", args = list("raw_b"))),
          list(op = list(type = "div", right = 2))
        )
      )
    ),
    outputs = "final_d"
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(
      raw_a = ts(5, start = c(2024, 1), frequency = 12),
      raw_b = ts(4, start = c(2024, 1), frequency = 12)
    ),
    transform_env = environment()
  )

  expect_s3_class(result, "PipelineResult")
  expect_s3_class(result$dag, "DAGPlan")
  expect_true(stats::is.ts(result$values$raw_a))
  expect_equal(as.numeric(result$values$mid_c), 50)
  expect_equal(as.numeric(result$values$final_d), 27)
  expect_equal(as.numeric(result$outputs$final_d), 27)
  expect_equal(result$plan$order, c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_equal(result$dag$node_status[["final_d"]], "ok")
})

test_that("evaluate_pipeline pipes a NULL step result as the first argument", {
  to_null <- function(x) NULL
  show_args <- function(x, k = "default") list(x = x, k = k)

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "to_null")),
          list(call = list(name = "show_args", args = list(list(literal = "second"))))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(1:3)),
    transform_env = environment(),
    progress = FALSE
  )

  expect_equal(result$values$b, list(x = NULL, k = "second"))
})

test_that("expand_node_result pipes a NULL step result as the first argument", {
  to_null <- function(x) NULL
  show_args <- function(x, k = "default") list(x = x, k = k)

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "to_null")),
          list(call = list(name = "show_args", args = list(list(literal = "second"))))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(1:3)),
    transform_env = environment(),
    progress = FALSE
  )
  expanded <- expand_node_result(result, "b", transform_env = environment())

  expect_null(expanded$failed_step)
  expect_equal(expanded$values$b, list(x = NULL, k = "second"))
})

test_that("evaluate_pipeline materializes all values when outputs are absent", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory", key = "raw_a")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(op = list(type = "mul", right = 3))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(
      raw_a = ts(7, start = c(2024, 1), frequency = 12)
    )
  )

  expect_equal(names(result$outputs), "derived_b")
  expect_equal(as.numeric(result$outputs$derived_b), 21)
  expect_equal(names(result$values), c("raw_a", "derived_b"))
})

test_that("expand_node_result replays a node step by step", {
  scale <- function(x, factor) x * factor
  add_n <- function(x, n) x + n

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "final_d",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10))),
          list(call = list(name = "add_n", args = list(5))),
          list(op = list(type = "div", right = 3))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(6, start = c(2024, 1), frequency = 12)),
    transform_env = environment(),
    progress = FALSE
  )

  expanded <- expand_node_result(result, "final_d", transform_env = environment())

  expect_s3_class(expanded, "ExpandedNodeResult")
  expect_null(expanded$failed_step)
  expect_equal(as.numeric(expanded$values$raw_a), 6)
  expect_equal(as.numeric(expanded$values[["final_d::step_1"]]), 60)
  expect_equal(as.numeric(expanded$values[["final_d::step_2"]]), 65)
  expect_equal(as.numeric(expanded$values[["final_d::step_3"]]), 65 / 3)
  expect_equal(as.numeric(expanded$values$final_d), 65 / 3)
})

test_that("default_output_names prefers derived nodes in the executed subgraph", {
  pipeline <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))
  dag <- build_dag(pipeline)

  expect_equal(default_output_names(pipeline, dag), "derived_b")
})

test_that("evaluate_pipeline prunes execution to the requested output subgraph", {
  touch_left <- function(x) x * 2
  touch_right <- function(x) stop("should not run", call. = FALSE)

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "raw_b", source = list(type = "memory")),
      list(
        name = "left_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "touch_left"))
        )
      ),
      list(
        name = "right_d",
        steps = list(
          list(input = "raw_b"),
          list(call = list(name = "touch_right"))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(
      raw_a = ts(3, start = c(2024, 1), frequency = 12),
      raw_b = ts(4, start = c(2024, 1), frequency = 12)
    ),
    outputs = "left_c",
    transform_env = environment()
  )

  expect_equal(result$plan$order, c("raw_a", "left_c"))
  expect_equal(names(result$values), c("raw_a", "left_c"))
  expect_equal(names(result$outputs), "left_c")
  expect_equal(as.numeric(result$outputs$left_c), 6)
})

test_that("evaluate_pipeline supports parallel frontier execution", {
  scale <- function(x, factor) x * factor
  testthat::skip_if_not_installed("future")
  testthat::skip_if_not_installed("future.apply")

  pkg_path <- getNamespaceInfo(asNamespace("tsflow"), "path")
  if (!file.exists(file.path(pkg_path, "Meta", "package.rds"))) {
    testthat::skip("parallel worker test requires tsflow to be installed")
  }

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "raw_b", source = list(type = "memory")),
      list(
        name = "left_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(2)))
        )
      ),
      list(
        name = "right_d",
        steps = list(
          list(input = "raw_b"),
          list(call = list(name = "scale", args = list(3)))
        )
      )
    )
  ))

  oplan <- future::plan()
  on.exit(future::plan(oplan), add = TRUE)
  future::plan(future::multisession, workers = 2)

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(
      raw_a = ts(3, start = c(2024, 1), frequency = 12),
      raw_b = ts(4, start = c(2024, 1), frequency = 12)
    ),
    transform_env = environment(),
    progress = FALSE
  )

  expect_equal(as.numeric(result$values$left_c), 6)
  expect_equal(as.numeric(result$values$right_d), 12)
})

test_that("evaluate_pipeline validates requested outputs and raw inputs at runtime", {
  identity_fn <- function(x) x

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory", key = "raw_a")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "identity_fn"))
        )
      )
    )
  ))

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = ts(1, start = c(2024, 1), frequency = 12)),
      outputs = "missing_output",
      transform_env = environment()
    ),
    "requested outputs are not present in the pipeline"
  )

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(),
      transform_env = environment()
    ),
    "raw input `raw_a` is not available in `inputs`"
  )

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = 1),
      transform_env = environment()
    ),
    "raw input `raw_a` must be an R `ts` or `xts` object"
  )

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = 1
    ),
    "`inputs` must be a named list of preloaded raw series values"
  )
})

test_that("evaluate_pipeline accepts xts raw inputs", {
  skip_if_not_installed("xts")

  spec <- list(
    series = list(
      list(name = "raw_a", kind = "raw", source = list(type = "injected")),
      list(
        name = "out",
        kind = "derived",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  )

  x <- xts::xts(1:3, order.by = as.Date(c("2024-01-01", "2024-01-02", "2024-01-03")))
  result <- evaluate_pipeline(
    pipeline = normalize_pipeline(spec),
    inputs = list(raw_a = x),
    outputs = "out",
    transform_env = environment()
  )

  expect_s3_class(result$outputs$out, "xts")
})

test_that("evaluate_pipeline rejects unsupported expression classes", {
  expect_error(
    evaluate_expr(structure(list(), class = "UnknownExpr"), values = list(), transform_env = baseenv()),
    "cannot evaluate unsupported expression class"
  )
})

test_that("evaluate_pipeline adds series and transform context to failures", {
  boom <- function(x) stop("kaboom", call. = FALSE)

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "boom"))
        )
      )
    )
  ))

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = ts(1, start = c(2024, 1), frequency = 12)),
      transform_env = environment()
    ),
    "failed while evaluating series `derived_b`: transform `boom` failed: kaboom"
  )
})

test_that("evaluate_pipeline collects node errors and skips downstream nodes when requested", {
  boom <- function(x) stop("kaboom", call. = FALSE)
  scale <- function(x, factor) x * factor

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "raw_b", source = list(type = "memory")),
      list(
        name = "left_fail",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "boom"))
        )
      ),
      list(
        name = "left_downstream",
        steps = list(
          list(input = "left_fail"),
          list(op = list(type = "mul", right = 2))
        )
      ),
      list(
        name = "right_ok",
        steps = list(
          list(input = "raw_b"),
          list(call = list(name = "scale", args = list(3)))
        )
      )
    ),
    outputs = c("left_downstream", "right_ok")
  ))

  result <- NULL
  expect_warning(
    result <- evaluate_pipeline(
      pipeline = spec,
      inputs = list(
        raw_a = ts(1, start = c(2024, 1), frequency = 12),
        raw_b = ts(2, start = c(2024, 1), frequency = 12)
      ),
      transform_env = environment(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expect_s3_class(result, "PipelineResult")
  expect_equal(result$error_mode, "collect")
  expect_equal(names(result$errors), "left_fail")
  expect_match(result$errors$left_fail, "transform `boom` failed: kaboom")
  expect_equal(result$skipped, "left_downstream")
  expect_equal(as.numeric(result$values$right_ok), 6)
  expect_false("left_fail" %in% names(result$values))
  expect_false("left_downstream" %in% names(result$values))
  expect_equal(names(result$outputs), "right_ok")
})

test_that("evaluate_pipeline keeps fail-fast behavior by default", {
  boom <- function(x) stop("kaboom", call. = FALSE)

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "derived_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "boom"))
        )
      )
    )
  ))

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = ts(1, start = c(2024, 1), frequency = 12)),
      transform_env = environment(),
      progress = FALSE
    ),
    "failed while evaluating series `derived_b`: transform `boom` failed: kaboom"
  )
})

test_that("expand_node_result marks a failed internal step and downstream skip", {
  scale <- function(x, factor) x * factor
  boom <- function(x) stop("kaboom", call. = FALSE)
  add_n <- function(x, n) x + n

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "final_d",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10))),
          list(call = list(name = "boom")),
          list(call = list(name = "add_n", args = list(5)))
        )
      )
    )
  ))

  result <- NULL
  expect_warning(
    result <- evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = ts(6, start = c(2024, 1), frequency = 12)),
      transform_env = environment(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expanded <- expand_node_result(result, "final_d", transform_env = environment())

  expect_equal(expanded$failed_step, "final_d::step_2")
  expect_equal(as.numeric(expanded$values$raw_a), 6)
  expect_equal(as.numeric(expanded$values[["final_d::step_1"]]), 60)
  expect_match(expanded$errors[["final_d::step_2"]], "transform `boom` failed: kaboom")
  expect_equal(expanded$dag$node_status[["final_d::step_2"]], "failed")
  expect_equal(expanded$dag$node_status[["final_d::step_3"]], "skipped")
  expect_equal(expanded$dag$node_status[["final_d"]], "skipped")
  expect_false("final_d" %in% names(expanded$values))
})

test_that("expand_node_result marks a child of a failed node as skipped", {
  boom <- function(x) stop("kaboom", call. = FALSE)
  add_n <- function(x, n) x + n

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(
        name = "parent_fail",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "boom"))
        )
      ),
      list(
        name = "child_node",
        steps = list(
          list(input = "parent_fail"),
          list(call = list(name = "add_n", args = list(5)))
        )
      )
    )
  ))

  result <- NULL
  expect_warning(
    result <- evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = ts(6, start = c(2024, 1), frequency = 12)),
      transform_env = environment(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expanded <- expand_node_result(result, "child_node", transform_env = environment())

  expect_s3_class(expanded, "ExpandedNodeResult")
  expect_null(expanded$failed_step)
  expect_match(expanded$errors[[1]], "was skipped because upstream input `parent_fail` is not available")
  expect_equal(expanded$dag$node_status[["parent_fail"]], "ok")
  expect_equal(expanded$dag$node_status[["child_node::step_1"]], "skipped")
  expect_equal(expanded$dag$node_status[["child_node"]], "skipped")
  expect_length(expanded$values, 0L)
})

test_that("result helpers expose the executed DAG and upstream path", {
  scale <- function(x, factor) x * factor
  add_n <- function(x, n) x + n

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "raw_b", source = list(type = "memory")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(10)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "add_n", args = list("raw_b")))
        )
      )
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(
      raw_a = ts(5, start = c(2024, 1), frequency = 12),
      raw_b = ts(4, start = c(2024, 1), frequency = 12)
    ),
    transform_env = environment()
  )

  dag <- result$dag
  subgraph <- result_subgraph(result, "final_d")

  expect_s3_class(dag, "DAGPlan")
  expect_equal(dag$order, c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_equal(result_path(result, "final_d"), c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_s3_class(subgraph, "DAGSubgraph")
  expect_equal(subgraph$nodes, c("raw_a", "raw_b", "mid_c", "final_d"))
})
