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
    transforms = local_transforms()
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
    transforms = local_transforms(),
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
    transforms = local_transforms(),
    progress = FALSE
  )
  expanded <- expand_node_result(result, "b", transforms = local_transforms())

  expect_null(expanded$failed_step)
  expect_equal(expanded$values$b, list(x = NULL, k = "second"))
})

test_that("evaluate_pipeline applies binary_op and postprocess hooks", {
  double_it <- function(x) x * 2

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(
        name = "b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "double_it")),
          list(op = list(type = "add", right = 1))
        )
      )
    )
  ))

  seen <- character()
  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(1:3)),
    transforms = list(double_it = double_it),
    hooks = list(
      binary_op = function(op, left, right) {
        seen <<- c(seen, op)
        left - right
      },
      postprocess = function(result, context) {
        seen <<- c(seen, context$transform_name)
        result * 10
      }
    ),
    progress = FALSE
  )

  # (1:3 * 2) * 10 via postprocess, then "add" replaced by subtraction
  expect_equal(as.numeric(result$values$b), c(19, 39, 59))
  expect_equal(seen, c("double_it", "add"))
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
    transforms = local_transforms(),
    progress = FALSE
  )

  expanded <- expand_node_result(result, "final_d", transforms = local_transforms())

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
    transforms = local_transforms()
  )

  expect_equal(result$plan$order, c("raw_a", "left_c"))
  expect_equal(names(result$values), c("raw_a", "left_c"))
  expect_equal(names(result$outputs), "left_c")
  expect_equal(as.numeric(result$outputs$left_c), 6)
})

test_that("evaluate_pipeline evaluates independent branches sequentially in DAG order", {
  scale <- function(x, factor) x * factor
  calls <- character()
  traced_scale <- function(x, factor) {
    calls <<- c(calls, as.character(factor))
    scale(x, factor)
  }

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "raw_b", source = list(type = "memory")),
      list(name = "left_c", steps = list(list(input = "raw_a"), list(call = list(name = "scale", args = list(2))))),
      list(name = "right_d", steps = list(list(input = "raw_b"), list(call = list(name = "scale", args = list(3)))))
    )
  ))

  result <- evaluate_pipeline(
    pipeline = spec,
    inputs = list(raw_a = ts(3), raw_b = ts(4)),
    transforms = list(scale = traced_scale),
    progress = FALSE
  )

  expect_equal(as.numeric(result$values$left_c), 6)
  expect_equal(as.numeric(result$values$right_d), 12)
  expect_equal(calls, c("2", "3"))
})

test_that("fail mode stops at the first error and collect mode continues past it", {
  boom <- function(x) stop("kaboom", call. = FALSE)
  scale <- function(x, factor) x * factor
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "memory")),
      list(name = "broken", expr = "boom(raw_a)"),
      list(name = "after", expr = "broken + 1"),
      list(name = "fine", expr = "scale(raw_a, factor = 2)")
    )
  ))
  inputs <- list(raw_a = ts(1:3))

  expect_error(
    evaluate_pipeline(spec, inputs, transforms = local_transforms(), progress = FALSE),
    "failed while evaluating series `broken`: transform `boom` failed: kaboom",
    fixed = TRUE
  )

  expect_warning(
    result <- evaluate_pipeline(spec, inputs, transforms = local_transforms(), progress = FALSE, error_mode = "collect"),
    "failed: broken"
  )
  expect_equal(names(result$errors), "broken")
  expect_equal(result$skipped, "after")
  expect_equal(as.numeric(result$values$fine), c(2, 4, 6))
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
      transforms = local_transforms()
    ),
    "requested outputs are not present in the pipeline"
  )

  expect_warning(
    missing_result <- evaluate_pipeline(
      pipeline = spec,
      inputs = list(),
      transforms = local_transforms()
    ),
    "missing: raw_a"
  )
  expect_equal(missing_result$missing_inputs, "raw_a")

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = list(raw_a = 1),
      transforms = local_transforms()
    ),
    "raw input `raw_a` must be an R `ts` or `xts` object"
  )

  expect_error(
    evaluate_pipeline(
      pipeline = spec,
      inputs = 1,
      transforms = local_transforms()
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
    transforms = local_transforms()
  )

  expect_s3_class(result$outputs$out, "xts")
})

test_that("evaluate_pipeline rejects unsupported expression classes", {
  expect_error(
    evaluate_expr(structure(list(), class = "UnknownExpr"), values = list(), runtime = new_runtime()),
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
      transforms = local_transforms()
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
      transforms = local_transforms(),
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
      transforms = local_transforms(),
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
      transforms = local_transforms(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expanded <- expand_node_result(result, "final_d", transforms = local_transforms())

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
      transforms = local_transforms(),
      progress = FALSE,
      error_mode = "collect"
    ),
    "Some pipeline nodes failed during execution"
  )

  expanded <- expand_node_result(result, "child_node", transforms = local_transforms())

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
    transforms = local_transforms()
  )

  dag <- result$dag
  subgraph <- result_subgraph(result, "final_d")

  expect_s3_class(dag, "DAGPlan")
  expect_equal(dag$order, c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_equal(result_path(result, "final_d"), c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_s3_class(subgraph, "DAGSubgraph")
  expect_equal(subgraph$nodes, c("raw_a", "raw_b", "mid_c", "final_d"))
})

test_that("evaluate_pipeline records a data hash for each raw input", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(name = "raw_b", source = list(type = "injected")),
      list(name = "total", expr = "raw_a + raw_b")
    )
  ))
  run <- function(raw_a, raw_b = ts(c(1, 1, 1), start = c(2024, 1), frequency = 4)) {
    evaluate_pipeline(spec, inputs = list(raw_a = raw_a, raw_b = raw_b), progress = FALSE)
  }
  raw_a <- ts(c(1, 2, 3), start = c(2024, 1), frequency = 4)

  result <- run(raw_a)
  expect_named(result$input_hashes, c("raw_a", "raw_b"))
  expect_match(result$input_hashes, "^[0-9a-f]{32}$")

  # Same data, same hash; attributes outside the data don't matter.
  attr(raw_a, "note") <- "ignored"
  expect_equal(run(raw_a)$input_hashes, result$input_hashes)

  changed_value <- run(ts(c(1, 2, 4), start = c(2024, 1), frequency = 4))$input_hashes
  expect_false(changed_value[["raw_a"]] == result$input_hashes[["raw_a"]])
  expect_equal(changed_value[["raw_b"]], result$input_hashes[["raw_b"]])

  shifted <- run(
    ts(c(1, 2, 3), start = c(2024, 2), frequency = 4),
    ts(c(1, 1, 1), start = c(2024, 2), frequency = 4)
  )$input_hashes
  expect_false(shifted[["raw_a"]] == result$input_hashes[["raw_a"]])
})

test_that("input hashes leave out missing inputs and work for xts", {
  skip_if_not_installed("xts")
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(name = "raw_b", source = list(type = "injected")),
      list(name = "total", expr = "raw_a + raw_b")
    )
  ))
  raw_a <- xts::xts(c(1, 2), order.by = as.Date(c("2024-01-01", "2024-02-01")))

  result <- evaluate_pipeline(spec, inputs = list(raw_a = raw_a), progress = FALSE) |>
    suppressWarnings()
  expect_named(result$input_hashes, "raw_a")
  expect_match(result$input_hashes[["raw_a"]], "^[0-9a-f]{32}$")
})
