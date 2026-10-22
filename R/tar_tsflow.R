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
#' @param hooks Optional named list of advanced evaluation hooks; see the
#'   [package hooks guide](../doc/package-hooks.html) or
#'   `vignette("package-hooks", package = "tsflow")`.
#' @param inputs Name of a target holding a named list of raw series values,
#'   keyed by raw series name, or `NULL` if all raw series come from loaders.
#'   Each raw series becomes its own target that takes its value from this
#'   list, or else from its loader.
#' @param loaders A named list of functions that load raw series, keyed by
#'   `source` type; see [evaluate_pipeline()]. Targets of raw series with a
#'   loader rerun on every `tar_make()`, because `targets` can't see whether
#'   external data changed; steps downstream rerun only if the loaded value
#'   changed.
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
#'   Namespaced transforms such as `tempdisagg::td`, and registered transforms,
#'   hooks and loaders defined in a package, record the package version, so
#'   updating the package reruns the steps that use it. For a package loaded
#'   with `pkgload::load_all()`, they also record a hash of all code in its
#'   namespace, so any edit to its R code reruns the steps that use the
#'   package, without a version bump. Edits to its compiled code or to data in
#'   `data/` still need a version bump.
#'
#'   A raw series that neither `inputs` nor a loader provides (or whose loader
#'   returns `NULL`) is a missing input. Its target and every target that
#'   depends on it hold a `tsflow_missing_input` placeholder instead of a
#'   value, so the rest of the pipeline still builds.
#'
#' @return A list of target objects.
#' @export
tar_tsflow <- function(path,
                       transforms = list(),
                       hooks = list(),
                       inputs = "tsflow_inputs",
                       granularity = c("step", "series"),
                       outputs = NULL,
                       loaders = list()) {
  if (!requireNamespace("targets", quietly = TRUE)) {
    fail("package `targets` is required for `tar_tsflow()`")
  }
  granularity <- match.arg(granularity)
  if (!is.null(inputs) && (!is_scalar_string(inputs) || !is_syntactic_name(inputs))) {
    fail("`inputs` must be the name of a target, as a single string, or NULL")
  }

  runtime <- new_runtime(transforms = transforms, hooks = hooks)
  assert_loaders(loaders)
  pipeline <- normalize_pipeline(read_pipeline(path))
  dag <- build_dag(pipeline, transforms = transforms)

  if (!is.null(inputs) && inputs %in% dag$nodes) {
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
  hook_refs <- tar_list_refs(hooks, substitute(hooks))
  loader_refs <- tar_list_refs(loaders, substitute(loaders))
  series_map <- stats::setNames(pipeline$series, vapply(pipeline$series, `[[`, character(1), "name"))
  # Hash each development package once, not once per target.
  fingerprints <- new.env(parent = emptyenv())

  targets_list <- lapply(nodes, function(name) {
    series <- series_map[[name]]
    if (identical(series$kind, "raw")) {
      return(list(tar_raw_target(name, series$source, inputs, loader_refs, loaders, fingerprints)))
    }

    if (identical(granularity, "series")) {
      return(list(targets::tar_target_raw(
        name,
        tar_series_command(series$expression, transform_refs, hook_refs, transforms, hooks, fingerprints)
      )))
    }

    tar_step_targets(name, series$expression, transform_refs, hook_refs, transforms, hooks, fingerprints)
  })

  unlist(targets_list, recursive = FALSE)
}

# `tsflow:::fn` for target commands. targets scans global functions for
# dependencies but not package namespaces, so commands call into tsflow by
# namespace rather than embedding function objects.
tsflow_internal <- function(fn) {
  call(":::", as.name("tsflow"), as.name(fn))
}

# A raw series target: from `inputs` if present, else its loader. Loaders
# read external data targets can't see, so those targets always rerun;
# downstream steps still only rerun when the loaded value changed.
tar_raw_target <- function(name, source, inputs, loader_refs, loaders = list(),
                           fingerprints = new.env(parent = emptyenv())) {
  loader_ref <- loader_refs[[source$type]]
  command <- bquote(.(tsflow_internal("tar_tsflow_raw"))(
    inputs = .(if (is.null(inputs)) NULL else as.name(inputs)),
    name = .(name),
    source = .(source$config),
    type = .(source$type),
    loader = .(loader_ref),
    packages = .(tar_package_versions(character(), loaders[source$type], fingerprints))
  ))
  cue <- if (is.null(loader_ref)) targets::tar_cue() else targets::tar_cue(mode = "always")
  targets::tar_target_raw(name, command, cue = cue)
}

tar_tsflow_raw <- function(inputs, name, source, type, loader, packages = NULL) {
  loaders <- if (is.null(loader)) list() else stats::setNames(list(loader), type)
  series <- list(name = name, source = list(type = type, config = source))
  loaded <- load_raw_series(series, inputs %||% list(), loaders)

  if (identical(loaded$status, "error")) {
    fail(loaded$error)
  }
  if (identical(loaded$status, "missing")) {
    return(new_missing_input(name))
  }
  loaded$value
}

# A missing input placeholder if any of `values` is one, else NULL.
tar_missing_from <- function(values) {
  missing <- Filter(is_missing_input, values)
  if (length(missing) == 0L) {
    return(NULL)
  }
  new_missing_input(unlist(lapply(missing, `[[`, "names"), use.names = FALSE))
}

tar_tsflow_series <- function(expression, values, transforms, hooks, packages = NULL) {
  missing <- tar_missing_from(values)
  if (!is.null(missing)) {
    return(missing)
  }
  evaluate_expr(expression, values = values, runtime = new_runtime(transforms, hooks))
}

tar_tsflow_step <- function(step, input, values, transforms, hooks, packages = NULL) {
  missing <- tar_missing_from(c(list(input), values))
  if (!is.null(missing)) {
    return(missing)
  }
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
  evaluate_binary_op(
    op = step$op,
    left = input,
    right = right,
    runtime = runtime,
    labels = c("left side", operand_label(step$right, "right side"))
  )
}

tar_step_targets <- function(name, expression, transform_refs, hook_refs, transforms = list(), hooks = list(),
                             fingerprints = new.env(parent = emptyenv())) {
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
      packages = .(tar_package_versions(used, c(
        transforms[intersect(used, names(transforms))],
        hooks[intersect(hooks_needed, names(hooks))]
      ), fingerprints))
    ))
    previous <<- step_names[[idx]]
    targets::tar_target_raw(step_names[[idx]], command)
  })
}

tar_series_command <- function(expression, transform_refs, hook_refs, transforms = list(), hooks = list(),
                               fingerprints = new.env(parent = emptyenv())) {
  used <- collect_transform_names(expression)
  bquote(.(tsflow_internal("tar_tsflow_series"))(
    expression = .(expression),
    values = .(tar_symbol_list(extract_dependencies(expression))),
    transforms = .(tar_ref_list(transform_refs, used)),
    hooks = .(tar_ref_list(hook_refs, names(hook_refs))),
    packages = .(tar_package_versions(used, c(
      transforms[intersect(used, names(transforms))], hooks
    ), fingerprints))
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

# Expressions referring to each entry of `hooks` / `loaders`: the element
# expressions when written inline as `list(...)`, else `<expr>[["name"]]`.
tar_list_refs <- function(entries, entries_expr) {
  if (length(entries) == 0L) {
    return(list())
  }

  if (is.call(entries_expr) && identical(entries_expr[[1L]], as.name("list"))) {
    return(as.list(entries_expr)[-1L][names(entries)])
  }

  stats::setNames(
    lapply(names(entries), function(name) bquote(.(entries_expr)[[.(name)]])),
    names(entries)
  )
}

# Package versions for namespaced transforms and registered functions, embedded
# in commands so package updates rerun the targets that use them.
# `fingerprints` caches code hashes of development packages by name.
tar_package_versions <- function(transform_names, functions = list(),
                                 fingerprints = new.env(parent = emptyenv())) {
  packages <- unique(sub("::.*$", "", grep("::", transform_names, fixed = TRUE, value = TRUE)))
  registered_packages <- vapply(functions, function(fn) {
    envir <- environment(fn)
    if (!is.null(envir) && isNamespace(envir)) getNamespaceName(envir) else NA_character_
  }, character(1))
  packages <- unique(c(packages, registered_packages[!is.na(registered_packages)]))
  if (length(packages) == 0L) {
    return(NULL)
  }
  stats::setNames(
    vapply(packages, tar_package_version, character(1), fingerprints = fingerprints),
    packages
  )
}

# The version of `pkg`, plus a hash of its code if it was loaded with
# `pkgload::load_all()` (which marks the namespace with `.__DEVTOOLS__`), as
# its code then changes without a version bump.
tar_package_version <- function(pkg, fingerprints) {
  version <- as.character(utils::packageVersion(pkg))
  if (!isNamespaceLoaded(pkg)) {
    return(version)
  }
  namespace <- asNamespace(pkg)
  if (!exists(".__DEVTOOLS__", envir = namespace, inherits = FALSE)) {
    return(version)
  }
  if (is.null(fingerprints[[pkg]])) {
    fingerprints[[pkg]] <- tar_namespace_hash(namespace)
  }
  paste0(version, "+", fingerprints[[pkg]])
}

# Hash of all bindings in a namespace. Hashing the whole namespace rather than
# what a function calls also covers S3 methods, helpers built by function
# factories and names looked up dynamically, at the cost of rerunning on any
# edit to the package.
tar_namespace_hash <- function(namespace) {
  names <- setdiff(
    sort(names(namespace), method = "radix"),
    c(".__DEVTOOLS__", ".__NAMESPACE__.", ".__S3MethodsTable__.")
  )
  values <- mget(names, envir = namespace)
  secretbase::sha3(lapply(values, tar_code_value, namespace = namespace))
}

# `value` reduced to what determines its behavior, for hashing. Functions are
# compared by code rather than serialized as closures, because R byte-compiles
# closures in place and source references hold file metadata. Environments in
# bindings (S4 method tables, R6 classes) contribute only the functions and
# environments they hold, as other values there are often mutable state;
# function environments contribute all their values, as for function factories.
tar_code_value <- function(value, namespace, seen = list(), state = FALSE) {
  if (is.primitive(value)) {
    return(value)
  }
  if (is.function(value)) {
    value <- utils::removeSource(value)
    return(list(
      formals(value),
      body(value),
      tar_code_value(environment(value), namespace, seen, state = TRUE)
    ))
  }
  if (is.environment(value)) {
    if (identical(value, namespace) || isNamespace(value) || identical(value, globalenv()) ||
        identical(value, baseenv()) || identical(value, emptyenv()) ||
        any(vapply(seen, identical, logical(1), value))) {
      return(environmentName(value))
    }
    seen <- c(seen, value)
    entries <- mget(sort(names(value), method = "radix"), envir = value)
    if (!state) {
      entries <- Filter(function(entry) is.function(entry) || is.environment(entry), entries)
    }
    return(lapply(entries, tar_code_value, namespace = namespace, seen = seen))
  }
  # Recurse into list elements and attributes (which hold S4 slots, as in class
  # definitions), so closures inside them are compared by code.
  attrs <- attributes(value)
  if (!is.list(value) && is.null(attrs)) {
    return(value)
  }
  attributes(value) <- NULL
  list(
    if (is.list(value)) lapply(value, tar_code_value, namespace = namespace, seen = seen) else value,
    lapply(attrs[sort(as.character(names(attrs)), method = "radix")], tar_code_value,
           namespace = namespace, seen = seen)
  )
}
