#' Inspect an Executed Pipeline Result
#'
#' Convenience helpers for inspecting the executed DAG and upstream calculation
#' path stored in a `PipelineResult`.
#'
#' @param result A `PipelineResult` object.
#' @param node A node name present in the executed DAG.
#' @param include_self Whether to include `node` in the returned result.
#' @param transforms A named list of functions used to resolve bare transform
#'   names during step-by-step replay of one expanded node. Use the same
#'   registry that produced `result`.
#' @param hooks Optional named list of evaluation hooks used during replay; see
#'   [evaluate_pipeline()].
#'
#' @return
#' `result_path()` returns the ordered upstream path used to calculate `node`.
#'
#' `result_subgraph()` returns a `DAGSubgraph` containing the upstream nodes and
#' edges used to calculate `node`.
#'
#' `expand_node_result()` returns an `ExpandedNodeResult` containing the
#' expanded step graph for one node, the value at each internal step, and any
#' step-level replay failure.
#'
#' @name result_helpers
#' @export
expand_node_result <- function(result, node, transforms = list(), hooks = list()) {
  assert_inherits(result, "PipelineResult", "result")
  runtime <- new_runtime(transforms = transforms, hooks = hooks)

  pipeline <- result$pipeline
  assert_inherits(pipeline, "PipelineSpec", "result$pipeline")

  dag <- result$dag
  assert_inherits(dag, "DAGPlan", "result$dag")
  assert_known_node(dag, node)

  series_map <- stats::setNames(pipeline$series, vapply(pipeline$series, `[[`, character(1), "name"))
  series <- series_map[[node]]
  if (is.null(series) || !identical(series$kind, "derived")) {
    fail(sprintf("node `%s` must be a derived series to expand step by step", node))
  }

  expanded_dag <- expand_node_dag(result, node)
  decomposition <- decompose_node_expression(series$expression)
  base_input <- decomposition$input

  if (!(base_input %in% names(result$values)) || is.null(result$values[[base_input]])) {
    expanded_dag <- expand_node_dag(result, node)
    expanded_dag$node_status <- expanded_node_status(expanded_dag, failed_step = NULL)
    expanded_dag$node_status[setdiff(expanded_dag$nodes, base_input)] <- "skipped"

    return(structure(
      list(
        node = node,
        dag = expanded_dag,
        values = list(),
        errors = list(node = sprintf(
          "node `%s` was skipped because upstream input `%s` is not available in `result$values`",
          node,
          base_input
        )),
        failed_step = NULL
      ),
      class = c("ExpandedNodeResult", "tsflow_spec")
    ))
  }

  step_values <- stats::setNames(vector("list", length(expanded_dag$nodes)), expanded_dag$nodes)
  step_values[[base_input]] <- result$values[[base_input]]

  errors <- list()
  failed_step <- NULL
  current_value <- result$values[[base_input]]

  if (length(decomposition$steps) > 0L) {
    for (idx in seq_along(decomposition$steps)) {
      step <- decomposition$steps[[idx]]
      step_id <- sprintf("%s::step_%d", node, idx)

      step_value <- tryCatch(
        evaluate_expanded_step(
          step = step,
          current_value = current_value,
          values = result$values,
          runtime = runtime
        ),
        error = function(e) e
      )

      if (inherits(step_value, "error")) {
        failed_step <- step_id
        errors[[step_id]] <- conditionMessage(step_value)
        break
      }

      current_value <- step_value
      step_values[[step_id]] <- current_value
    }
  }

  if (is.null(failed_step)) {
    step_values[[node]] <- current_value
  } else {
    errors[[node]] <- sprintf("node replay stopped at `%s`", failed_step)
  }

  expanded_dag$node_status <- expanded_node_status(expanded_dag, failed_step = failed_step)

  structure(
    list(
      node = node,
      dag = expanded_dag,
      values = step_values[!vapply(step_values, is.null, logical(1))],
      errors = errors,
      failed_step = failed_step
    ),
    class = c("ExpandedNodeResult", "tsflow_spec")
  )
}

#' @rdname result_helpers
#' @export
result_path <- function(result, node, include_self = TRUE) {
  assert_inherits(result, "PipelineResult", "result")
  assert_inherits(result$dag, "DAGPlan", "result$dag")
  upstream_nodes(result$dag, node, include_self = include_self)
}

#' @rdname result_helpers
#' @export
result_subgraph <- function(result, node, include_self = TRUE) {
  assert_inherits(result, "PipelineResult", "result")
  assert_inherits(result$dag, "DAGPlan", "result$dag")
  dag_subgraph(result$dag, upstream_of = node, include_focus = include_self)
}

decompose_node_expression <- function(expr) {
  if (inherits(expr, "RefNode")) {
    return(list(
      input = expr$name,
      steps = list()
    ))
  }

  if (inherits(expr, "PipeNode")) {
    upstream <- decompose_node_expression(expr$input)
    return(list(
      input = upstream$input,
      steps = c(upstream$steps, list(list(type = "call", node = expr$call)))
    ))
  }

  if (inherits(expr, "BinaryOpNode")) {
    upstream <- decompose_node_expression(expr$left)
    return(list(
      input = upstream$input,
      steps = c(upstream$steps, list(list(type = "op", node = expr)))
    ))
  }

  fail(sprintf(
    "cannot decompose unsupported expression class `%s`",
    paste(class(expr), collapse = ", ")
  ))
}

evaluate_expanded_step <- function(step, current_value, values, runtime) {
  if (identical(step$type, "call")) {
    call_node <- step$node
    return(invoke_transform(
      transform_name = call_node$name,
      input_value = current_value,
      args = call_node$args,
      kwargs = call_node$kwargs,
      values = values,
      runtime = runtime
    ))
  }

  if (identical(step$type, "op")) {
    op_node <- step$node
    right <- evaluate_expr(op_node$right, values = values, runtime = runtime)

    # Same arithmetic as evaluation (hooks and alignment checks included).
    return(evaluate_binary_op(
      op = op_node$op,
      left = current_value,
      right = right,
      runtime = runtime,
      labels = c(operand_label(op_node$left, "left side"), operand_label(op_node$right, "right side"))
    ))
  }

  fail(sprintf("unsupported expanded step type `%s`", step$type))
}

expanded_node_status <- function(dag, failed_step = NULL) {
  node_status <- stats::setNames(rep("ok", length(dag$nodes)), dag$nodes)

  if (is.null(failed_step)) {
    return(node_status)
  }

  failed_idx <- match(failed_step, dag$nodes)
  if (!is.na(failed_idx)) {
    node_status[[failed_step]] <- "failed"
    if (failed_idx < length(dag$nodes)) {
      skipped <- dag$nodes[seq.int(failed_idx + 1L, length(dag$nodes))]
      node_status[skipped] <- "skipped"
    }
  }

  node_status
}
