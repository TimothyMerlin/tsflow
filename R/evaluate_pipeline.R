#' Evaluate a Pipeline
#'
#' Evaluates a normalized pipeline in topological order using preloaded raw
#' inputs and direct R function resolution.
#'
#' @param pipeline A `PipelineSpec` object.
#' @param inputs A named list of preloaded raw series values. Raw inputs may be
#'   R `ts` or `xts` objects keyed by raw series name.
#' @param outputs Optional output names to materialize. When provided, execution
#'   is pruned to the upstream subgraph required to compute those outputs.
#' @param transforms A named list of functions used to resolve bare transform
#'   names during validation and execution. Namespaced transforms such as
#'   `stats::lag` are resolved directly and do not need to be registered.
#' @param hooks Optional named list of evaluation hooks for advanced runtimes.
#'   Supported hooks are `binary_op` and `postprocess`; see Details.
#' @param progress Whether to show execution progress in interactive sessions.
#'   Defaults to `getOption("tsflow.progress", TRUE)`.
#' @param error_mode Execution failure mode. `"fail"` stops on the first node
#'   error. `"collect"` continues evaluating unaffected nodes, records node-level
#'   failures, and skips downstream nodes whose dependencies failed.
#'
#' @return A `PipelineResult` object containing the DAG plan, all computed
#'   values, the materialized outputs, and optional error metadata.
#'
#' @details Transform names may be authored either as namespaced references
#'   such as `stats::lag` or as bare names registered in `transforms`, e.g.
#'   `transforms = list(scale = scale)`.
#'
#'   `hooks$binary_op(op, left, right)`, when supplied, evaluates `add`, `sub`,
#'   `mul` and `div` steps instead of the default arithmetic.
#'   `hooks$postprocess(result, context)`, when supplied, is called on every
#'   transform result after the transform function returns.
#'
#'   In interactive sessions, `evaluate_pipeline()` uses `progressr`
#'   automatically when it is installed. Existing user-configured `progressr`
#'   handlers are respected; if none are configured, tsflow installs a default
#'   handler for the current call.
#'
#' @export
evaluate_pipeline <- function(pipeline,
                              inputs,
                              outputs = pipeline$outputs %||% NULL,
                              transforms = list(),
                              hooks = list(),
                              progress = getOption("tsflow.progress", TRUE),
                              error_mode = c("fail", "collect")) {
  error_mode <- match.arg(error_mode)
  runtime <- new_runtime(transforms = transforms, hooks = hooks)
  dag <- build_dag(pipeline, transforms = transforms)

  if (!is.null(outputs)) {
    unknown_outputs <- setdiff(unique(outputs), dag$nodes)
    if (length(unknown_outputs) > 0L) {
      fail(sprintf(
        "requested outputs are not present in the pipeline: %s",
        paste(unknown_outputs, collapse = ", ")
      ))
    }
  }

  if (!is.list(inputs) || (length(inputs) > 0L && is.null(names(inputs)))) {
    fail("`inputs` must be a named list of preloaded raw series values")
  }

  execution_dag <- prune_execution_dag(dag, outputs = outputs)
  values <- stats::setNames(vector("list", length(execution_dag$nodes)), execution_dag$nodes)
  series_map <- stats::setNames(pipeline$series, vapply(pipeline$series, `[[`, character(1), "name"))

  execution_result <- run_with_execution_progress(
    execution_dag = execution_dag,
    values = values,
    series_map = series_map,
    inputs = inputs,
    runtime = runtime,
    progress = progress,
    error_mode = error_mode
  )
  new_pipeline_result(
    pipeline = pipeline,
    execution_dag = execution_dag,
    values = execution_result$values,
    outputs = outputs,
    errors = execution_result$errors,
    skipped = execution_result$skipped,
    error_mode = error_mode
  )
}

# Shared by in-memory and cached (targets) execution.
new_pipeline_result <- function(pipeline,
                                execution_dag,
                                values,
                                outputs,
                                errors,
                                skipped,
                                error_mode,
                                extra = list()) {
  if (identical(error_mode, "collect") && length(errors) > 0L) {
    lines <- c(
      "Some pipeline nodes failed during execution",
      "x" = paste("failed:", paste(names(errors), collapse = ", "))
    )

    if (length(skipped) > 0L) {
      lines <- c(
        lines,
        "i" = paste("skipped:", paste(skipped, collapse = ", "))
      )
    }

    warn(lines)
  }

  materialized_names <- outputs %||% default_output_names(pipeline, execution_dag)
  available_outputs <- materialized_names[materialized_names %in% names(values)]

  structure(
    c(
      list(
        pipeline = pipeline,
        plan = execution_dag,
        dag = annotate_result_dag(dag = execution_dag, errors = errors, skipped = skipped),
        values = values,
        outputs = values[available_outputs],
        errors = errors,
        skipped = skipped,
        error_mode = error_mode
      ),
      extra
    ),
    class = c("PipelineResult", "tsflow_spec")
  )
}

annotate_result_dag <- function(dag, errors = list(), skipped = character()) {
  assert_inherits(dag, "DAGPlan", "dag")

  node_status <- stats::setNames(rep("ok", length(dag$nodes)), dag$nodes)
  failed_nodes <- names(errors %||% list())
  skipped_nodes <- skipped %||% character()

  if (length(failed_nodes) > 0L) {
    node_status[failed_nodes] <- "failed"
  }
  if (length(skipped_nodes) > 0L) {
    node_status[skipped_nodes] <- "skipped"
  }

  dag$node_status <- node_status
  dag
}

default_output_names <- function(pipeline, execution_dag) {
  derived_names <- vapply(
    pipeline$series,
    function(item) {
      if (identical(item$kind, "derived") && item$name %in% execution_dag$nodes) {
        item$name
      } else {
        NA_character_
      }
    },
    character(1)
  )

  derived_names <- derived_names[!is.na(derived_names)]
  if (length(derived_names) > 0L) {
    return(unname(derived_names))
  }

  execution_dag$nodes
}

run_with_execution_progress <- function(execution_dag,
                                        values,
                                        series_map,
                                        inputs,
                                        runtime,
                                        progress,
                                        error_mode) {
  if (!is_scalar_flag(progress)) {
    fail("`progress` must be TRUE or FALSE")
  }

  if (interactive() && progress && requireNamespace("progressr", quietly = TRUE)) {
    handlers <- tryCatch(progressr::handlers(), error = function(e) list())

    runner <- function() {
      tick <- progressr::progressor(steps = length(execution_dag$order))
      evaluate_execution_nodes(
        execution_dag = execution_dag,
        values = values,
        series_map = series_map,
        inputs = inputs,
        runtime = runtime,
        tick = function(name) tick(message = sprintf("evaluated %s", name)),
        error_mode = error_mode
      )
    }

    if (length(handlers) > 0L) {
      return(progressr::with_progress(runner()))
    }

    return(progressr::with_progress(
      runner(),
      handlers = default_execution_handler()
    ))
  }

  evaluate_execution_nodes(
    execution_dag = execution_dag,
    values = values,
    series_map = series_map,
    inputs = inputs,
    runtime = runtime,
    tick = function(name) invisible(NULL),
    error_mode = error_mode
  )
}

evaluate_execution_nodes <- function(execution_dag,
                                     values,
                                     series_map,
                                     inputs,
                                     runtime,
                                     tick,
                                     error_mode) {
  errors <- list()
  skipped <- character()

  for (batch in execution_batches(execution_dag)) {
    if (identical(error_mode, "collect")) {
      batch_results <- evaluate_execution_batch_collecting(
        batch = batch,
        execution_dag = execution_dag,
        values = values,
        series_map = series_map,
        inputs = inputs,
        runtime = runtime
      )

      for (name in batch) {
        item <- batch_results[[name]]

        if (identical(item$status, "ok")) {
          values[[name]] <- item$value
          tick(name)
        } else if (identical(item$status, "error")) {
          errors[[name]] <- item$error
        } else if (identical(item$status, "skipped")) {
          skipped <- c(skipped, name)
        }
      }

      next
    }

    batch_results <- evaluate_execution_batch(
      batch = batch,
      values = values,
      series_map = series_map,
      inputs = inputs,
      runtime = runtime
    )

    for (name in batch) {
      values[[name]] <- batch_results[[name]]
      tick(name)
    }
  }

  list(
    values = values[!vapply(values, is.null, logical(1))],
    errors = errors,
    skipped = unname(skipped)
  )
}

evaluate_execution_batch <- function(batch,
                                     values,
                                     series_map,
                                     inputs,
                                     runtime) {
  if (length(batch) > 1L &&
      requireNamespace("future.apply", quietly = TRUE) &&
      requireNamespace("future", quietly = TRUE) &&
      future_plan_allows_parallel()) {
    result <- future.apply::future_lapply(
      batch,
      function(name) {
        evaluate_series_node(
          name = name,
          series = series_map[[name]],
          values = values,
          inputs = inputs,
          runtime = runtime
        )
      }
    )
    names(result) <- batch
    return(result)
  }

  result <- lapply(
    batch,
    function(name) {
      evaluate_series_node(
        name = name,
        series = series_map[[name]],
        values = values,
        inputs = inputs,
        runtime = runtime
      )
    }
  )
  names(result) <- batch
  result
}

evaluate_execution_batch_collecting <- function(batch,
                                                execution_dag,
                                                values,
                                                series_map,
                                                inputs,
                                                runtime) {
  if (length(batch) > 1L &&
      requireNamespace("future.apply", quietly = TRUE) &&
      requireNamespace("future", quietly = TRUE) &&
      future_plan_allows_parallel()) {
    result <- future.apply::future_lapply(
      batch,
      function(name) {
        evaluate_series_node_collecting(
          name = name,
          series = series_map[[name]],
          values = values,
          inputs = inputs,
          runtime = runtime,
          dependencies = execution_dag$dependencies[[name]] %||% character()
        )
      }
    )
    names(result) <- batch
    return(result)
  }

  result <- lapply(
    batch,
    function(name) {
      evaluate_series_node_collecting(
        name = name,
        series = series_map[[name]],
        values = values,
        inputs = inputs,
        runtime = runtime,
        dependencies = execution_dag$dependencies[[name]] %||% character()
      )
    }
  )
  names(result) <- batch
  result
}

evaluate_series_node <- function(name,
                                 series,
                                 values,
                                 inputs,
                                 runtime) {
  tryCatch(
    {
      if (identical(series$kind, "raw")) {
        resolve_input_series(name, inputs)
      } else {
        evaluate_expr(
          expr = series$expression,
          values = values,
          runtime = runtime
        )
      }
    },
    error = function(e) {
      fail(sprintf(
        "failed while evaluating series `%s`: %s",
        name,
        conditionMessage(e)
      ))
    }
  )
}

evaluate_series_node_collecting <- function(name,
                                            series,
                                            values,
                                            inputs,
                                            runtime,
                                            dependencies) {
  missing_deps <- dependencies[!dependencies %in% names(values) | vapply(values[dependencies], is.null, logical(1))]
  if (length(missing_deps) > 0L) {
    return(list(
      status = "skipped",
      dependencies = missing_deps
    ))
  }

  tryCatch(
    list(
      status = "ok",
      value = if (identical(series$kind, "raw")) {
        resolve_input_series(name, inputs)
      } else {
        evaluate_expr(
          expr = series$expression,
          values = values,
          runtime = runtime
        )
      }
    ),
    error = function(e) {
      list(
        status = "error",
        error = conditionMessage(e)
      )
    }
  )
}

default_execution_handler <- function() {
  cli_handler <- get0("handler_cli", envir = asNamespace("progressr"), mode = "function")
  if (is.function(cli_handler)) {
    return(cli_handler())
  }

  progressr::handler_txtprogressbar()
}

future_plan_allows_parallel <- function() {
  plan_value <- tryCatch(future::plan(), error = function(e) NULL)
  if (is.null(plan_value)) {
    return(FALSE)
  }

  if (is.list(plan_value) && length(plan_value) > 0L) {
    plan_value <- plan_value[[1L]]
  }

  classes <- class(plan_value)
  !any(classes %in% c("sequential", "SequentialFutureBackend", "SequentialFuture"))
}

execution_batches <- function(dag) {
  assert_inherits(dag, "DAGPlan", "dag")

  remaining <- stats::setNames(
    vapply(dag$nodes, function(name) length(dag$dependencies[[name]]), integer(1)),
    dag$nodes
  )
  ready <- dag$order[remaining[dag$order] == 0L]
  batches <- list()
  completed <- character()

  while (length(ready) > 0L) {
    batch <- ready
    batches[[length(batches) + 1L]] <- batch
    completed <- c(completed, batch)
    ready <- character()

    for (node in batch) {
      for (child in dag$downstream[[node]]) {
        remaining[[child]] <- remaining[[child]] - 1L
      }
    }

    ready <- dag$order[remaining[dag$order] == 0L & !(dag$order %in% completed)]
  }

  batches
}

prune_execution_dag <- function(dag, outputs = NULL) {
  assert_inherits(dag, "DAGPlan", "dag")

  if (is.null(outputs)) {
    return(dag)
  }

  required_nodes <- character()
  for (node in unique(outputs)) {
    required_nodes <- c(required_nodes, upstream_nodes(dag, node, include_self = TRUE))
  }
  required_nodes <- order_nodes(dag, required_nodes)

  dependencies <- stats::setNames(vector("list", length(required_nodes)), required_nodes)
  edge_labels <- stats::setNames(
    replicate(length(required_nodes), setNames(character(), character()), simplify = FALSE),
    required_nodes
  )
  downstream <- stats::setNames(
    replicate(length(required_nodes), character(), simplify = FALSE),
    required_nodes
  )

  for (name in required_nodes) {
    deps <- dag$dependencies[[name]]
    keep_deps <- deps[deps %in% required_nodes]
    dependencies[[name]] <- keep_deps

    labels <- dag$edge_labels[[name]]
    if (length(labels) > 0L) {
      edge_labels[[name]] <- labels[names(labels) %in% keep_deps]
    }
  }

  for (name in required_nodes) {
    deps <- dependencies[[name]]
    for (dep in deps) {
      downstream[[dep]] <- c(downstream[[dep]], name)
    }
  }

  structure(
    list(
      nodes = required_nodes,
      dependencies = dependencies,
      edge_labels = edge_labels,
      downstream = downstream,
      node_kind = dag$node_kind[required_nodes],
      node_role = dag$node_role[required_nodes],
      node_pipeline = dag$node_pipeline[required_nodes],
      order = required_nodes
    ),
    class = class(dag)
  )
}

resolve_input_series <- function(name, inputs) {
  if (!(name %in% names(inputs)) || is.null(inputs[[name]])) {
    fail(sprintf("raw input `%s` is not available in `inputs`", name))
  }

  value <- inputs[[name]]
  if (!(stats::is.ts(value) || inherits(value, "xts"))) {
    fail(sprintf("raw input `%s` must be an R `ts` or `xts` object", name))
  }

  value
}

evaluate_expr <- function(expr, values, runtime) {
  if (inherits(expr, "RefNode")) {
    if (!(expr$name %in% names(values)) || is.null(values[[expr$name]])) {
      fail(sprintf("referenced value `%s` is not available", expr$name))
    }
    return(values[[expr$name]])
  }

  if (inherits(expr, "LiteralNode")) {
    return(expr$value)
  }

  if (inherits(expr, "CallNode")) {
    return(invoke_transform(
      transform_name = expr$name,
      args = expr$args,
      kwargs = expr$kwargs,
      values = values,
      runtime = runtime
    ))
  }

  if (inherits(expr, "PipeNode")) {
    input_value <- evaluate_expr(expr$input, values = values, runtime = runtime)
    call_node <- expr$call
    return(invoke_transform(
      transform_name = call_node$name,
      input_value = input_value,
      args = call_node$args,
      kwargs = call_node$kwargs,
      values = values,
      runtime = runtime
    ))
  }

  if (inherits(expr, "BinaryOpNode")) {
    left <- evaluate_expr(expr$left, values = values, runtime = runtime)
    right <- evaluate_expr(expr$right, values = values, runtime = runtime)
    return(evaluate_binary_op(
      op = expr$op,
      left = left,
      right = right,
      runtime = runtime
    ))
  }

  fail(sprintf(
    "cannot evaluate unsupported expression class `%s`",
    paste(class(expr), collapse = ", ")
  ))
}

invoke_transform <- function(transform_name,
                             args,
                             kwargs,
                             values,
                             runtime,
                             input_value) {
  # Pipe steps always pass `input_value`, even when the upstream step returned
  # NULL, so only a missing argument means there is no piped value.
  has_input <- !missing(input_value)
  if (!has_input) {
    input_value <- NULL
  }

  tryCatch(
    {
      fn <- resolve_transform(transform_name, transforms = runtime$transforms)
      evaluated_args <- lapply(args, evaluate_expr, values = values, runtime = runtime)
      evaluated_kwargs <- lapply(kwargs, evaluate_expr, values = values, runtime = runtime)
      call_args <- c(if (has_input) list(input_value), evaluated_args, evaluated_kwargs)
      postprocess_evaluated_value(
        do.call(fn, call_args),
        runtime = runtime,
        context = list(
          kind = "transform",
          transform_name = transform_name,
          input_value = input_value,
          args = evaluated_args,
          kwargs = evaluated_kwargs
        )
      )
    },
    error = function(e) {
      fail(sprintf(
        "transform `%s` failed: %s",
        transform_name,
        conditionMessage(e)
      ))
    }
  )
}

evaluate_binary_op <- function(op, left, right, runtime) {
  if (!is.null(runtime$hooks$binary_op)) {
    return(runtime$hooks$binary_op(op = op, left = left, right = right))
  }

  if (identical(op, "add")) {
    return(left + right)
  }
  if (identical(op, "sub")) {
    return(left - right)
  }
  if (identical(op, "mul")) {
    return(left * right)
  }
  if (identical(op, "div")) {
    return(left / right)
  }

  fail(sprintf("unsupported binary op `%s`", op))
}

postprocess_evaluated_value <- function(result, runtime, context) {
  if (is.null(runtime$hooks$postprocess)) {
    return(result)
  }

  runtime$hooks$postprocess(result = result, context = context)
}
