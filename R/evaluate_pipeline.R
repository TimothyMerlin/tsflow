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
#' @param transform_env Environment used to resolve bare transform names during
#'   execution. Namespaced transforms such as `stats::lag` are resolved
#'   directly.
#' @param progress Whether to show execution progress in interactive sessions.
#'   Defaults to `getOption("tsflow.progress", TRUE)`.
#' @param error_mode Execution failure mode. `"fail"` stops on the first node
#'   error. `"collect"` continues evaluating unaffected nodes, records node-level
#'   failures, and skips downstream nodes whose dependencies failed.
#'
#' @return A `PipelineResult` object containing the DAG plan, all computed
#'   values, the materialized outputs, and optional error metadata.
#'
#' @details Transform names may be authored either as explicit namespaced
#'   references such as `stats::lag` or as bare names resolved from
#'   `transform_env`. The recommended authored form is `pkg::fn`.
#'
#'   `transform_env` may also define optional evaluation hooks for advanced
#'   runtimes. If a function named `tsflow_evaluate_binary_op(op, left, right)`
#'   is present, tsflow uses it to evaluate DAG binary operations instead of the
#'   default arithmetic dispatch. If a function named
#'   `tsflow_postprocess_value(result, context)` is present, tsflow calls it on
#'   transform results after the underlying transform function returns. When
#'   these hooks are absent, default evaluation behavior is unchanged.
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
                              transform_env = parent.frame(),
                              progress = getOption("tsflow.progress", TRUE),
                              error_mode = c("fail", "collect")) {
  error_mode <- match.arg(error_mode)
  dag <- build_dag(pipeline, transform_env = transform_env)

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
    transform_env = transform_env,
    progress = progress,
    error_mode = error_mode
  )
  values <- execution_result$values

  if (identical(error_mode, "collect") && length(execution_result$errors) > 0L) {
    failed_nodes <- names(execution_result$errors)
    skipped_nodes <- execution_result$skipped

    lines <- c(
      "Some pipeline nodes failed during execution",
      "x" = paste("failed:", paste(failed_nodes, collapse = ", "))
    )

    if (length(skipped_nodes) > 0L) {
      lines <- c(
        lines,
        "i" = paste("skipped:", paste(skipped_nodes, collapse = ", "))
      )
    }

    warn(lines)
  }

  materialized_names <- outputs %||% default_output_names(pipeline, execution_dag)
  available_outputs <- materialized_names[materialized_names %in% names(values)]
  materialized <- values[available_outputs]
  annotated_dag <- annotate_result_dag(
    dag = execution_dag,
    errors = execution_result$errors,
    skipped = execution_result$skipped
  )

  structure(
    list(
      pipeline = pipeline,
      plan = execution_dag,
      dag = annotated_dag,
      values = values,
      outputs = materialized,
      errors = execution_result$errors,
      skipped = execution_result$skipped,
      error_mode = error_mode
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
                                        transform_env,
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
        transform_env = transform_env,
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
    transform_env = transform_env,
    tick = function(name) invisible(NULL),
    error_mode = error_mode
  )
}

evaluate_execution_nodes <- function(execution_dag,
                                     values,
                                     series_map,
                                     inputs,
                                     transform_env,
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
        transform_env = transform_env
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
      transform_env = transform_env
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
                                     transform_env) {
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
          transform_env = transform_env
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
        transform_env = transform_env
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
                                                transform_env) {
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
          transform_env = transform_env,
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
        transform_env = transform_env,
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
                                 transform_env) {
  tryCatch(
    {
      if (identical(series$kind, "raw")) {
        resolve_input_series(name, inputs)
      } else {
        evaluate_expr(
          expr = series$expression,
          values = values,
          transform_env = transform_env
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
                                            transform_env,
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
          transform_env = transform_env
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

evaluate_expr <- function(expr, values, transform_env) {
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
      transform_env = transform_env
    ))
  }

  if (inherits(expr, "PipeNode")) {
    input_value <- evaluate_expr(expr$input, values = values, transform_env = transform_env)
    call_node <- expr$call
    return(invoke_transform(
      transform_name = call_node$name,
      input_value = input_value,
      args = call_node$args,
      kwargs = call_node$kwargs,
      values = values,
      transform_env = transform_env
    ))
  }

  if (inherits(expr, "BinaryOpNode")) {
    left <- evaluate_expr(expr$left, values = values, transform_env = transform_env)
    right <- evaluate_expr(expr$right, values = values, transform_env = transform_env)
    return(evaluate_binary_op(
      op = expr$op,
      left = left,
      right = right,
      transform_env = transform_env
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
                             transform_env,
                             input_value) {
  # Pipe steps always pass `input_value`, even when the upstream step returned
  # NULL, so only a missing argument means there is no piped value.
  has_input <- !missing(input_value)
  if (!has_input) {
    input_value <- NULL
  }

  tryCatch(
    {
      fn <- resolve_transform(transform_name, transform_env = transform_env)
      evaluated_args <- lapply(args, evaluate_expr, values = values, transform_env = transform_env)
      evaluated_kwargs <- lapply(kwargs, evaluate_expr, values = values, transform_env = transform_env)
      call_args <- c(if (has_input) list(input_value), evaluated_args, evaluated_kwargs)
      postprocess_evaluated_value(
        do.call(fn, call_args),
        transform_env = transform_env,
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

evaluate_binary_op <- function(op, left, right, transform_env) {
  if (exists("tsflow_evaluate_binary_op", envir = transform_env, mode = "function", inherits = TRUE)) {
    hook <- get("tsflow_evaluate_binary_op", envir = transform_env, mode = "function", inherits = TRUE)
    return(hook(op = op, left = left, right = right))
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

postprocess_evaluated_value <- function(result, transform_env, context) {
  if (!exists("tsflow_postprocess_value", envir = transform_env, mode = "function", inherits = TRUE)) {
    return(result)
  }

  hook <- get("tsflow_postprocess_value", envir = transform_env, mode = "function", inherits = TRUE)
  hook(result = result, context = context)
}
