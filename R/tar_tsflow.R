#' Run a Pipeline as a targets Pipeline
#'
#' Reads and validates a pipeline definition and returns a list of
#' [targets](https://docs.ropensci.org/targets/) target objects, one per step
#' (or per series), for use in `_targets.R`. `targets` then decides what to
#' rebuild, stores results between sessions and can run steps in parallel.
#'
#' @param path Path to a pipeline definition file.
#' @param transforms A named list of functions used to resolve bare transform
#'   names, as in [evaluate_pipeline()]. See Details for how changes to
#'   transforms are tracked.
#' @param hooks Optional named list of evaluation hooks; see
#'   [evaluate_pipeline()].
#' @param inputs Name of a target holding a named list of raw series values,
#'   keyed by raw series name. Each raw series becomes its own target that
#'   extracts its value from this list.
#' @param granularity `"step"` (default) creates one target per step, so a
#'   change only reruns the affected steps. `"series"` creates one target per
#'   series, which has less overhead for large pipelines of cheap transforms.
#' @param outputs Optional series names. When given, only targets needed to
#'   compute them are created.
#'
#' @details With `granularity = "step"`, the last step of a series is named
#'   after the series, so `targets::tar_read(gdp.real)` returns the series.
#'   Earlier steps are named `<series>..step<k>`.
#'
#'   For `targets` to rerun exactly the steps whose transform changed, each
#'   step refers to its transform by a name `targets` can track:
#'
#'   - If `transforms` is written inline, e.g.
#'     `transforms = list(scale = scale, growth = my_growth)`, each step uses
#'     the expression given for its transform (`scale`, `my_growth`).
#'   - If `transforms` is a variable, a step uses the global function with the
#'     transform's name when it is identical to the registry entry. Otherwise
#'     it refers to the whole registry, so any change to it reruns all steps;
#'     a message lists the affected transforms.
#'
#'   Namespaced transforms such as `tempdisagg::td` record the package
#'   version, so updating the package reruns the steps that use it.
#'
#' @return A list of target objects.
#' @export
tar_tsflow <- function(path,
                       transforms = list(),
                       hooks = list(),
                       inputs = "tsflow_inputs",
                       granularity = c("step", "series"),
                       outputs = NULL) {
  if (!requireNamespace("targets", quietly = TRUE)) {
    fail("package `targets` is required for `tar_tsflow()`")
  }
  granularity <- match.arg(granularity)
  if (!is_scalar_string(inputs) || !is_syntactic_name(inputs)) {
    fail("`inputs` must be the name of a target, as a single string")
  }

  runtime <- new_runtime(transforms = transforms, hooks = hooks)
  pipeline <- normalize_pipeline(read_pipeline(path))
  dag <- build_dag(pipeline, transforms = transforms)

  if (inputs %in% dag$nodes) {
    fail(sprintf("`inputs` target `%s` has the same name as a series", inputs))
  }

  nodes <- dag$order
  if (!is.null(outputs)) {
    unknown <- setdiff(outputs, dag$nodes)
    if (length(unknown) > 0L) {
      fail(sprintf("`outputs` are not series in the pipeline: %s", paste(unknown, collapse = ", ")))
    }
    needed <- unlist(lapply(outputs, upstream_nodes, dag = dag, include_self = TRUE))
    nodes <- order_nodes(dag, needed)
  }

  envir <- targets::tar_option_get("envir")
  transform_refs <- tar_transform_refs(transforms, substitute(transforms), envir)
  hook_refs <- tar_hook_refs(hooks, substitute(hooks))
  series_map <- stats::setNames(pipeline$series, vapply(pipeline$series, `[[`, character(1), "name"))

  targets_list <- lapply(nodes, function(name) {
    series <- series_map[[name]]
    if (identical(series$kind, "raw")) {
      return(list(targets::tar_target_raw(
        name,
        bquote(.(tsflow_internal("tar_tsflow_input"))(.(as.name(inputs)), .(name)))
      )))
    }

    if (identical(granularity, "series")) {
      return(list(targets::tar_target_raw(
        name,
        tar_series_command(series$expression, transform_refs, hook_refs)
      )))
    }

    tar_step_targets(name, series$expression, transform_refs, hook_refs)
  })

  unlist(targets_list, recursive = FALSE)
}

# `tsflow:::fn` for target commands. targets scans global functions for
# dependencies but not package namespaces, so commands call into tsflow by
# namespace rather than embedding function objects.
tsflow_internal <- function(fn) {
  call(":::", as.name("tsflow"), as.name(fn))
}

tar_tsflow_input <- function(inputs, name) {
  resolve_input_series(name, inputs)
}

tar_tsflow_series <- function(expression, values, transforms, hooks, packages = NULL) {
  evaluate_expr(expression, values = values, runtime = new_runtime(transforms, hooks))
}

tar_tsflow_step <- function(step, input, values, transforms, hooks, packages = NULL) {
  runtime <- new_runtime(transforms, hooks)

  if (identical(step$type, "call")) {
    return(invoke_transform(
      transform_name = step$name,
      args = step$args,
      kwargs = step$kwargs,
      values = values,
      runtime = runtime,
      input_value = input
    ))
  }

  right <- evaluate_expr(step$right, values = values, runtime = runtime)
  evaluate_binary_op(op = step$op, left = input, right = right, runtime = runtime)
}

tar_step_targets <- function(name, expression, transform_refs, hook_refs) {
  decomposition <- decompose_node_expression(expression)
  steps <- decomposition$steps

  if (length(steps) == 0L) {
    return(list(targets::tar_target_raw(name, as.name(decomposition$input))))
  }

  step_names <- c(sprintf("%s..step%d", name, seq_len(length(steps) - 1L)), name)
  previous <- decomposition$input

  lapply(seq_along(steps), function(idx) {
    step <- steps[[idx]]
    node <- step$node

    # Embed only this step's own content, not the upstream chain.
    if (identical(step$type, "call")) {
      step_spec <- list(type = "call", name = node$name, args = node$args, kwargs = node$kwargs)
      used <- c(node$name, unlist(lapply(c(node$args, node$kwargs), collect_transform_names), use.names = FALSE))
      deps <- extract_dependencies(node)
      hooks_needed <- "postprocess"
    } else {
      step_spec <- list(type = "op", op = node$op, right = node$right)
      used <- collect_transform_names(node$right)
      deps <- extract_dependencies(node$right)
      hooks_needed <- c("binary_op", if (length(used) > 0L) "postprocess")
    }

    command <- bquote(.(tsflow_internal("tar_tsflow_step"))(
      step = .(step_spec),
      input = .(as.name(previous)),
      values = .(tar_symbol_list(deps)),
      transforms = .(tar_ref_list(transform_refs, used)),
      hooks = .(tar_ref_list(hook_refs, hooks_needed)),
      packages = .(tar_package_versions(used))
    ))
    previous <<- step_names[[idx]]
    targets::tar_target_raw(step_names[[idx]], command)
  })
}

tar_series_command <- function(expression, transform_refs, hook_refs) {
  used <- collect_transform_names(expression)
  bquote(.(tsflow_internal("tar_tsflow_series"))(
    expression = .(expression),
    values = .(tar_symbol_list(extract_dependencies(expression))),
    transforms = .(tar_ref_list(transform_refs, used)),
    hooks = .(tar_ref_list(hook_refs, names(hook_refs))),
    packages = .(tar_package_versions(used))
  ))
}

# `list(a = a, b = b)` so targets sees each referenced series as a dependency.
tar_symbol_list <- function(names) {
  names <- unique(names)
  as.call(c(as.name("list"), stats::setNames(lapply(names, as.name), names)))
}

tar_ref_list <- function(refs, names) {
  names <- intersect(unique(names), names(refs))
  as.call(c(as.name("list"), refs[names]))
}

# Expressions that steps use to refer to each transform, chosen so targets can
# track changes to that transform specifically where possible.
tar_transform_refs <- function(transforms, transforms_expr, envir) {
  if (length(transforms) == 0L) {
    return(list())
  }

  if (is.call(transforms_expr) && identical(transforms_expr[[1L]], as.name("list"))) {
    refs <- as.list(transforms_expr)[-1L]
    return(refs[names(transforms)])
  }

  refs <- list()
  whole_registry <- character()
  for (name in names(transforms)) {
    if (exists(name, envir = envir, mode = "function", inherits = FALSE) &&
        identical(get(name, envir = envir, mode = "function"), transforms[[name]])) {
      refs[[name]] <- as.name(name)
    } else {
      refs[[name]] <- bquote(.(transforms_expr)[[.(name)]])
      whole_registry <- c(whole_registry, name)
    }
  }

  if (length(whole_registry) > 0L) {
    cli::cli_inform(escape_cli_braces(sprintf(
      "tar_tsflow(): transforms %s have no global function of the same name, so a change to any transform reruns all steps that use them.",
      paste(sprintf("`%s`", whole_registry), collapse = ", ")
    )))
  }

  refs
}

tar_hook_refs <- function(hooks, hooks_expr) {
  if (length(hooks) == 0L) {
    return(list())
  }

  if (is.call(hooks_expr) && identical(hooks_expr[[1L]], as.name("list"))) {
    return(as.list(hooks_expr)[-1L][names(hooks)])
  }

  stats::setNames(lapply(names(hooks), function(name) bquote(.(hooks_expr)[[.(name)]])), names(hooks))
}

# Package versions for namespaced transforms, embedded in the command so a
# package update changes the command and reruns the step.
tar_package_versions <- function(transform_names) {
  packages <- unique(sub("::.*$", "", grep("::", transform_names, fixed = TRUE, value = TRUE)))
  if (length(packages) == 0L) {
    return(NULL)
  }
  stats::setNames(
    vapply(packages, function(pkg) as.character(utils::packageVersion(pkg)), character(1)),
    packages
  )
}
