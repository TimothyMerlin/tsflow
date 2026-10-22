# Cached execution for `execute_pipeline(store = ...)`: build a targets
# pipeline with tar_tsflow() and run it in the current session, so only steps
# whose inputs, parameters or transforms changed are recomputed.
execute_pipeline_cached <- function(path,
                                    inputs,
                                    outputs,
                                    transforms,
                                    hooks,
                                    progress,
                                    error_mode,
                                    store,
                                    loaders = list()) {
  for (pkg in c("targets", "codetools")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      fail(sprintf("package `%s` is required for `execute_pipeline(store = ...)`", pkg))
    }
  }
  if (!is_scalar_string(store) || !nzchar(store)) {
    fail("`store` must be a single directory path")
  }
  if (!is_scalar_flag(progress)) {
    fail("`progress` must be TRUE or FALSE")
  }

  # Validate everything up front so errors read as they do without `store`.
  new_runtime(transforms = transforms, hooks = hooks)
  assert_loaders(loaders)
  pipeline <- normalize_pipeline(read_pipeline(path))
  outputs <- outputs %||% pipeline$outputs %||% NULL
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
  pipeline_store <- pipeline_store_path(store, path)

  # The generated script sets targets' `error` option in this session;
  # restore the caller's value afterwards.
  previous_error_option <- targets::tar_option_get("error")
  on.exit(targets::tar_option_set(error = previous_error_option), add = TRUE)
  envir <- cached_pipeline_envir(inputs, transforms, hooks, loaders)
  script <- tempfile(fileext = ".R")
  on.exit(unlink(script), add = TRUE)
  writeLines(
    cached_pipeline_script(path, transforms, hooks, outputs, error_mode, loaders),
    script
  )

  run_error <- tryCatch(
    {
      targets::tar_make(
        script = script,
        store = pipeline_store,
        callr_function = NULL,
        envir = envir,
        reporter = if (interactive() && progress) "balanced" else "silent"
      )
      NULL
    },
    error = function(e) e
  )

  progress_table <- targets::tar_progress(store = pipeline_store)
  meta <- targets::tar_meta(store = pipeline_store, fields = "error")
  target_errors <- meta$error[!is.na(meta$error)]
  names(target_errors) <- meta$name[!is.na(meta$error)]
  target_errors <- target_errors[names(target_errors) %in% progress_table$name[progress_table$progress == "errored"]]

  # With error = "continue", series downstream of a failure also error
  # because their input is missing; report those as skipped, not failed.
  errored_series <- unique(target_series_name(names(target_errors)))
  blocked <- unique(unlist(lapply(errored_series, downstream_nodes, dag = execution_dag)))
  failed_series <- setdiff(errored_series, blocked)
  skipped <- order_nodes(execution_dag, setdiff(blocked, failed_series))

  if (!is.null(run_error)) {
    if (length(failed_series) == 0L || identical(error_mode, "fail")) {
      if (length(failed_series) > 0L) {
        fail(sprintf(
          "failed while evaluating series `%s`: %s",
          failed_series[[1L]],
          target_errors[[1L]]
        ))
      }
      fail(sprintf("cached execution failed: %s", conditionMessage(run_error)))
    }
  }

  errors <- stats::setNames(
    lapply(failed_series, function(series) {
      unname(target_errors[target_series_name(names(target_errors)) == series][[1L]])
    }),
    failed_series
  )
  ok_series <- setdiff(execution_dag$order, c(failed_series, skipped))

  values <- stats::setNames(
    lapply(ok_series, targets::tar_read_raw, store = pipeline_store),
    ok_series
  )

  # Missing-input placeholders: raw ones are the missing inputs, derived ones
  # were skipped because of them.
  placeholder <- vapply(values, is_missing_input, logical(1))
  raw_series <- execution_dag$order[execution_dag$node_kind[execution_dag$order] == "raw"]
  missing_inputs <- intersect(names(values)[placeholder], raw_series)
  skipped <- order_nodes(execution_dag, c(skipped, setdiff(names(values)[placeholder], raw_series)))
  values <- values[!placeholder]

  # Raw series re-extract whenever the inputs list changes, so `rebuilt`
  # reports derived series only.
  completed <- progress_table$name[progress_table$progress == "completed"]
  derived <- execution_dag$order[execution_dag$node_kind[execution_dag$order] == "derived"]
  rebuilt <- intersect(derived, unique(target_series_name(completed)))

  new_pipeline_result(
    pipeline = pipeline,
    execution_dag = execution_dag,
    values = values,
    outputs = outputs,
    errors = errors,
    skipped = skipped,
    error_mode = error_mode,
    missing_inputs = missing_inputs,
    extra = list(rebuilt = rebuilt, store = pipeline_store)
  )
}

# One store subfolder per pipeline file, so pipelines sharing series names
# don't overwrite each other's results: `<store>/<file name>-<path hash>`.
pipeline_store_path <- function(store, path) {
  hash <- substr(md5_text(normalizePath(path, mustWork = TRUE)), 1L, 6L)

  stem <- gsub("[^A-Za-z0-9_.-]", "_", tools::file_path_sans_ext(basename(path)))
  file.path(store, paste0(stem, "-", hash))
}

# Strip the `..step<k>` suffix of intermediate step targets.
target_series_name <- function(target_names) {
  sub("\\.\\.step[0-9]+$", "", target_names)
}

# Private environment the generated targets script runs in: inputs,
# transforms, hooks, and every workspace function or value they use (followed
# recursively), so targets notices changes to helpers as well as transforms.
cached_pipeline_envir <- function(inputs, transforms, hooks, loaders = list()) {
  envir <- new.env(parent = globalenv())
  envir$tsflow_inputs_value <- inputs

  for (name in names(transforms)) {
    assign(name, transforms[[name]], envir = envir)
  }
  for (name in names(hooks)) {
    assign(paste0("tsflow_hook_", name), hooks[[name]], envir = envir)
  }
  for (name in names(loaders)) {
    assign(loader_var_name(name), loaders[[name]], envir = envir)
  }

  pending <- c(unname(transforms), unname(hooks), unname(loaders))
  seen <- character()
  while (length(pending) > 0L) {
    fn <- pending[[1L]]
    pending <- pending[-1L]

    for (global in codetools::findGlobals(fn)) {
      if (global %in% seen) {
        next
      }
      seen <- c(seen, global)

      home <- defining_env(global, environment(fn))
      if (is.null(home) || !is_workspace_env(home)) {
        next
      }
      value <- get(global, envir = home, inherits = FALSE)
      if (!exists(global, envir = envir, inherits = FALSE)) {
        assign(global, value, envir = envir)
      }
      if (is.function(value)) {
        pending <- c(pending, list(value))
      }
    }
  }

  envir
}

defining_env <- function(name, envir) {
  while (!identical(envir, emptyenv())) {
    if (exists(name, envir = envir, inherits = FALSE)) {
      return(envir)
    }
    envir <- parent.env(envir)
  }
  NULL
}

# Workspace = the global environment or a function frame, not a package or
# namespace (package code is tracked by version, or by code hash for
# `load_all()` packages, instead).
is_workspace_env <- function(envir) {
  !isNamespace(envir) &&
    !identical(envir, baseenv()) &&
    !startsWith(environmentName(envir), "package:")
}

cached_pipeline_script <- function(path, transforms, hooks, outputs, error_mode, loaders = list()) {
  transform_list <- paste(
    sprintf("%s = %s", vapply(names(transforms), quote_name, character(1)), vapply(names(transforms), quote_name, character(1))),
    collapse = ", "
  )
  hook_list <- paste(
    sprintf("%s = tsflow_hook_%s", names(hooks), names(hooks)),
    collapse = ", "
  )
  loader_list <- paste(
    sprintf("%s = %s", vapply(names(loaders), quote_name, character(1)), vapply(names(loaders), loader_var_name, character(1))),
    collapse = ", "
  )

  c(
    sprintf(
      "targets::tar_option_set(error = %s)",
      deparse(if (identical(error_mode, "collect")) "continue" else "stop")
    ),
    "list(",
    "  targets::tar_target_raw(\"tsflow_inputs\", quote(tsflow_inputs_value)),",
    "  tsflow::tar_tsflow(",
    sprintf("    %s,", deparse(normalizePath(path, mustWork = TRUE))),
    sprintf("    transforms = list(%s),", transform_list),
    sprintf("    hooks = list(%s),", hook_list),
    sprintf("    loaders = list(%s),", loader_list),
    sprintf("    outputs = %s", paste(deparse(outputs), collapse = "")),
    "  )",
    ")"
  )
}

loader_var_name <- function(type) {
  paste0("tsflow_loader_", make.names(type))
}

quote_name <- function(name) {
  if (is_syntactic_name(name)) name else sprintf("`%s`", name)
}
