#' Evaluate a Pipeline
#'
#' Evaluates a normalized pipeline in topological order using preloaded raw
#' inputs and direct R function resolution.
#'
#' @param pipeline A `PipelineSpec` object, or a `PipelinePlan` from
#'   [plan_pipeline()]. A plan is run as it was inspected: its dependency graph
#'   and planned outputs are reused (unless `outputs` is given).
#' @param inputs A named list of preloaded raw series values (`ts` or `xts`),
#'   keyed by raw series name. A series given here is used instead of its
#'   loader.
#' @param outputs Optional output names to materialize. When provided, execution
#'   is pruned to the upstream subgraph required to compute those outputs.
#' @param transforms A named list of functions used to resolve bare transform
#'   names during validation and execution. Namespaced transforms such as
#'   `stats::lag` are resolved directly and do not need to be registered.
#' @param hooks Optional named list of advanced evaluation hooks; see the
#'   [package hooks guide](../doc/package-hooks.html) or
#'   `vignette("package-hooks", package = "tsflow")`.
#' @param progress Whether to show execution progress in interactive sessions.
#'   Defaults to `getOption("tsflow.progress", TRUE)`.
#' @param error_mode Execution failure mode. `"fail"` stops on the first node
#'   error. `"collect"` continues evaluating unaffected nodes, records node-level
#'   failures, and skips downstream nodes whose dependencies failed.
#' @param loaders A named list of functions that load raw series, keyed by
#'   `source` type. Each is called as `loader(source, name)`, where `source` is
#'   the series' `source` entry without `type`, and returns a `ts` or `xts`
#'   object, or `NULL` when the data is not available.
#'
#' @return A `PipelineResult` object containing the DAG plan, all computed
#'   values, the materialized outputs, and optional error metadata.
#'
#' @details A raw series comes from `inputs` if it is there, otherwise from the
#'   loader for its `source` type. If neither provides it, or the loader
#'   returns `NULL`, the series is a missing input: it and every series that
#'   depends on it are skipped with a warning, and the rest of the pipeline
#'   still runs, in both error modes. Missing series are listed in
#'   `result$missing_inputs` and have status `"missing_input"` in
#'   `result$dag$node_status`. A loader that throws an error is a failure like
#'   any other step.
#'
#'   **Input hashes.** `result$input_hashes` is a named character vector with
#'   an MD5 hash of each raw series the run used. The hash covers the data
#'   only (class, frequency, time index, values and column names), so the same
#'   data gives the same hash in any session or R version. Compare hashes
#'   across runs to check whether the input data changed.
#'
#'   **Time alignment.** `add`, `sub`, `mul` and `div` steps between two series
#'   use R's arithmetic, which combines `ts` and `xts` series on their common
#'   period: observations outside the overlap are dropped without a warning.
#'   Series that don't overlap at all, `ts` series with different frequencies,
#'   and a `ts` combined with an `xts` are errors. Combining a series with a
#'   number is not checked. Transforms handle alignment themselves.
#'
#'   Transform names may be authored either as namespaced references
#'   such as `stats::lag` or as bare names registered in `transforms`, e.g.
#'   `transforms = list(scale = scale)`. Any installed package may be used for
#'   namespaced transforms; to restrict them, for example when running
#'   pipeline files from other people, set
#'   `options(tsflow.allowed_packages = c("stats", "tempdisagg"))`.
#'
#'   In interactive sessions, `evaluate_pipeline()` uses `progressr`
#'   automatically when it is installed. Existing user-configured `progressr`
#'   handlers are respected; if none are configured, tsflow installs a default
#'   handler for the current call.
#'
#' @export
evaluate_pipeline <- function(pipeline,
                              inputs = list(),
                              outputs = pipeline$outputs %||% NULL,
                              transforms = list(),
                              hooks = list(),
                              progress = getOption("tsflow.progress", TRUE),
                              error_mode = c("fail", "collect"),
                              loaders = list()) {
  error_mode <- match.arg(error_mode)
  runtime <- new_runtime(transforms = transforms, hooks = hooks)
  assert_loaders(loaders)

  # A plan is run as inspected: its DAG and planned outputs are reused, and
  # only the transforms given now are checked against it.
  if (inherits(pipeline, "PipelinePlan")) {
    plan <- pipeline
    pipeline <- plan$pipeline
    if (missing(outputs)) {
      outputs <- plan$outputs
    }
    if (!isTRUE(plan$dag$is_acyclic)) {
      fail(sprintf(
        "cannot evaluate a plan whose dependency graph contains a cycle involving: %s",
        paste(plan$dag$cycle_nodes, collapse = ", ")
      ))
    }
    validate_pipeline(pipeline, transforms = transforms)
    dag <- plan$dag
  } else {
    dag <- build_dag(pipeline, transforms = transforms)
  }

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
  series_map <- stats::setNames(pipeline$series, vapply(pipeline$series, `[[`, character(1), "name"))

  # Load raw series first. Missing ones (and load failures in "collect" mode)
  # are left out together with everything downstream; the rest still runs.
  raw_nodes <- execution_dag$order[execution_dag$node_kind[execution_dag$order] == "raw"]
  loaded <- load_raw_inputs(raw_nodes, series_map, inputs, loaders)
  if (identical(error_mode, "fail") && length(loaded$errors) > 0L) {
    fail(sprintf(
      "failed while evaluating series `%s`: %s",
      names(loaded$errors)[[1L]],
      loaded$errors[[1L]]
    ))
  }

  unavailable <- c(loaded$missing, names(loaded$errors))
  blocked <- setdiff(
    unique(unlist(lapply(unavailable, downstream_nodes, dag = execution_dag))),
    unavailable
  )
  run_nodes <- setdiff(execution_dag$order, c(unavailable, blocked))

  execution_result <- list(values = list(), errors = list(), skipped = character())
  if (length(run_nodes) > 0L) {
    run_dag <- prune_execution_dag(execution_dag, outputs = run_nodes)
    execution_result <- run_with_execution_progress(
      execution_dag = run_dag,
      values = stats::setNames(vector("list", length(run_dag$nodes)), run_dag$nodes),
      series_map = series_map,
      inputs = loaded$values,
      runtime = runtime,
      progress = progress,
      error_mode = error_mode
    )
  }

  new_pipeline_result(
    pipeline = pipeline,
    execution_dag = execution_dag,
    values = execution_result$values,
    outputs = outputs,
    errors = c(loaded$errors, execution_result$errors),
    skipped = order_nodes(execution_dag, c(blocked, execution_result$skipped)),
    error_mode = error_mode,
    missing_inputs = loaded$missing
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
                                missing_inputs = character(),
                                extra = list()) {
  # Missing inputs never stop a run, so always say what was left out.
  if (length(missing_inputs) > 0L) {
    missing_skipped <- intersect(
      skipped,
      unique(unlist(lapply(missing_inputs, downstream_nodes, dag = execution_dag)))
    )
    lines <- c(
      "Some raw inputs are missing; they and the series depending on them were skipped",
      "x" = paste("missing:", paste(missing_inputs, collapse = ", "))
    )
    if (length(missing_skipped) > 0L) {
      lines <- c(lines, "i" = paste("skipped:", paste(missing_skipped, collapse = ", ")))
    }
    warn(lines)
  }

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

  # Fingerprint the raw data the run used, so results can be traced to it.
  raw_nodes <- execution_dag$order[execution_dag$node_kind[execution_dag$order] == "raw"]
  raw_values <- Filter(Negate(is.null), values[intersect(raw_nodes, names(values))])
  input_hashes <- vapply(raw_values, series_hash, character(1))
  names(input_hashes) <- names(raw_values)

  structure(
    c(
      list(
        pipeline = pipeline,
        plan = execution_dag,
        dag = annotate_result_dag(
          dag = execution_dag,
          errors = errors,
          skipped = skipped,
          missing_inputs = missing_inputs
        ),
        values = values,
        outputs = values[available_outputs],
        errors = errors,
        skipped = skipped,
        missing_inputs = missing_inputs,
        input_hashes = input_hashes,
        error_mode = error_mode
      ),
      extra
    ),
    class = c("PipelineResult", "tsflow_spec")
  )
}

annotate_result_dag <- function(dag, errors = list(), skipped = character(), missing_inputs = character()) {
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
  if (length(missing_inputs) > 0L) {
    node_status[missing_inputs] <- "missing_input"
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

  # One sequential path for both modes: "fail" stops at the first error,
  # "collect" records it and skips the nodes that depend on it.
  for (name in execution_dag$order) {
    item <- evaluate_series_node(
      name = name,
      series = series_map[[name]],
      values = values,
      inputs = inputs,
      runtime = runtime,
      dependencies = execution_dag$dependencies[[name]] %||% character()
    )

    if (identical(item$status, "ok")) {
      values[[name]] <- item$value
      tick(name)
    } else if (identical(item$status, "error")) {
      if (identical(error_mode, "fail")) {
        fail(sprintf("failed while evaluating series `%s`: %s", name, item$error))
      }
      errors[[name]] <- item$error
    } else {
      skipped <- c(skipped, name)
    }
  }

  list(
    values = values[!vapply(values, is.null, logical(1))],
    errors = errors,
    skipped = unname(skipped)
  )
}

# Returns list(status = "ok" | "error" | "skipped", value, error).
evaluate_series_node <- function(name,
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
      runtime = runtime,
      labels = c(operand_label(expr$left, "left side"), operand_label(expr$right, "right side"))
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

evaluate_binary_op <- function(op, left, right, runtime, labels = c("left side", "right side")) {
  if (!is.null(runtime$hooks$binary_op)) {
    return(runtime$hooks$binary_op(op = op, left = left, right = right))
  }

  check_aligned_operands(op, left, right, labels)

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
