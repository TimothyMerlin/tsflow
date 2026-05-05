#' Normalize Pipeline Structures
#'
#' Converts parsed YAML or CSV-derived structures into the canonical tsflow
#' internal model.
#'
#' The MVP normalizer supports raw series defined by `source` and derived series
#' defined by structured `steps`. Derived expressions are normalized into
#' reference, literal, call, pipe, and binary operator nodes.
#'
#' @param x An R object representing a pipeline component.
#' @param steps A list of MVP `steps` entries.
#' @param vars Optional named list of shared atomic literal variables used to
#'   resolve explicit `var` references inside step arguments.
#'
#' @return
#' `normalize_pipeline()` returns a `PipelineSpec` object.
#'
#' `normalize_series()` returns a `SeriesSpec` object.
#'
#' `normalize_source()` returns a `SourceSpec` object.
#'
#' `normalize_steps()` and `normalize_step_arg()` return expression nodes.
#'
#' @name normalize_pipeline
#' @export
normalize_pipeline <- function(x) {
  assert_named_list(x, "pipeline")

  if (is.null(x$series)) {
    fail("pipeline must define `series`")
  }
  if (!is.list(x$series) || length(x$series) == 0L) {
    fail("pipeline `series` must be a non-empty list")
  }

  vars <- x$vars %||% NULL
  if (!is.null(vars)) {
    if (!is.list(vars) || is.null(names(vars))) {
      fail("pipeline `vars` must be a named list when present")
    }
    invalid_vars <- !vapply(vars, is_literal_value, logical(1))
    if (any(invalid_vars)) {
      fail(sprintf(
        "pipeline `vars` entries must be literal values: %s",
        paste(names(vars)[invalid_vars], collapse = ", ")
      ))
    }
  }

  series <- lapply(x$series, normalize_series, vars = vars)

  outputs <- x$outputs %||% NULL
  if (!is.null(outputs) && !is.character(outputs)) {
    fail("pipeline `outputs` must be a character vector when present")
  }

  metadata <- x$metadata %||% NULL
  if (!is.null(metadata) && !is.list(metadata)) {
    fail("pipeline `metadata` must be a map/list when present")
  }

  new_pipeline_spec(
    series = series,
    outputs = outputs,
    metadata = metadata,
    vars = vars
  )
}

#' @rdname normalize_pipeline
#' @export
normalize_series <- function(x, vars = NULL) {
  assert_named_list(x, "series entry")

  name <- x$name
  if (!is_scalar_string(name)) {
    fail("series entry `name` must be a single string")
  }

  has_source <- !is.null(x$source)
  has_steps <- !is.null(x$steps)

  if (has_source && has_steps) {
    fail(sprintf("series `%s` cannot define both `source` and `steps`", name))
  }
  if (!has_source && !has_steps) {
    fail(sprintf("series `%s` must define either `source` or `steps`", name))
  }

  description <- x$description %||% NULL
  if (!is.null(description) && !is_scalar_string(description)) {
    fail(sprintf("series `%s` `description` must be a single string", name))
  }

  metadata <- x$metadata %||% NULL
  if (!is.null(metadata) && !is.list(metadata)) {
    fail(sprintf("series `%s` `metadata` must be a map/list", name))
  }

  if (has_source) {
    new_series_spec(
      name = name,
      kind = "raw",
      source = normalize_source(x$source),
      expression = NULL,
      description = description,
      metadata = metadata
    )
  } else {
    new_series_spec(
      name = name,
      kind = "derived",
      source = NULL,
      expression = normalize_steps(x$steps, vars = vars),
      description = description,
      metadata = metadata
    )
  }
}

#' @rdname normalize_pipeline
#' @export
normalize_source <- function(x) {
  assert_named_list(x, "source")

  type <- x$type
  if (!is_scalar_string(type)) {
    fail("source `type` must be a single string")
  }

  config <- x[setdiff(names(x), "type")]
  if (is.null(config)) {
    config <- list()
  }

  new_source_spec(type = type, config = config)
}

#' @rdname normalize_pipeline
#' @export
normalize_steps <- function(steps, vars = NULL) {
  if (!is.list(steps) || length(steps) == 0L) {
    fail("`steps` must be a non-empty list")
  }

  input_positions <- which(vapply(
    steps,
    function(step) is.list(step) && !is.null(step$input),
    logical(1)
  ))

  if (length(input_positions) != 1L) {
    fail("`steps` must contain exactly one `input` step")
  }
  if (input_positions[[1L]] != 1L) {
    fail("the `input` step must be first")
  }

  current <- normalize_input_step(steps[[1L]])

  if (length(steps) == 1L) {
    return(current)
  }

  for (idx in 2:length(steps)) {
    step <- steps[[idx]]
    assert_named_list(step, sprintf("step %d", idx))

    step_keys <- names(step)
    if (length(step_keys) != 1L) {
      fail(sprintf("step %d must define exactly one step type", idx))
    }

    if (!is.null(step$input)) {
      fail("`input` can only appear as the first step")
    }

    if (!is.null(step$call)) {
      current <- normalize_call_step(step$call, current, idx, vars = vars)
      next
    }

    if (!is.null(step$op)) {
      current <- normalize_op_step(step$op, current, idx, vars = vars)
      next
    }

    fail(sprintf("step %d has unsupported type `%s`", idx, step_keys[[1L]]))
  }

  current
}

#' @rdname normalize_pipeline
#' @export
normalize_step_arg <- function(x, vars = NULL) {
  if (is.null(x)) {
    return(new_literal_node(NULL))
  }

  if (is_scalar_number(x) || is_scalar_flag(x)) {
    return(new_literal_node(x))
  }

  if (is_scalar_string(x)) {
    return(new_ref_node(x))
  }

  if (is.list(x) && identical(names(x), "literal")) {
    if (!is_literal_value(x$literal)) {
      fail("explicit `literal` step arg must contain a literal value")
    }
    return(new_literal_node(x$literal))
  }

  if (is.list(x) && identical(names(x), "ref")) {
    if (!is_scalar_string(x$ref)) {
      fail("explicit `ref` step arg must contain a single string")
    }
    return(new_ref_node(x$ref))
  }

  if (is.list(x) && identical(names(x), "var")) {
    if (!is_scalar_string(x$var)) {
      fail("explicit `var` step arg must contain a single string")
    }
    if (is.null(vars) || !(x$var %in% names(vars))) {
      fail(sprintf("step arg references unknown var `%s`", x$var))
    }
    return(new_literal_node(vars[[x$var]]))
  }

  fail("step args must normalize to a literal value or series reference")
}

normalize_input_step <- function(step) {
  assert_named_list(step, "input step")

  if (length(names(step)) != 1L || is.null(step$input)) {
    fail("the first step must be an `input` step")
  }

  input <- step$input
  if (!is_scalar_string(input)) {
    fail("`input` must reference a single series name")
  }

  new_ref_node(input)
}

normalize_call_step <- function(call, current, idx, vars = NULL) {
  assert_named_list(call, sprintf("call step %d", idx))

  name <- call$name
  if (!is_scalar_string(name)) {
    fail(sprintf("call step %d `name` must be a single string", idx))
  }

  args <- call$args %||% list()
  if (!is.list(args)) {
    args <- list(args)
  }
  normalized_args <- lapply(args, normalize_step_arg, vars = vars)

  kwargs <- call$kwargs %||% list()
  if (!is.list(kwargs)) {
    fail(sprintf("call step %d `kwargs` must be a named list", idx))
  }
  if (length(kwargs) > 0L && is.null(names(kwargs))) {
    fail(sprintf("call step %d `kwargs` must be a named list", idx))
  }
  normalized_kwargs <- lapply(kwargs, normalize_step_arg, vars = vars)

  new_pipe_node(
    input = current,
    call = new_call_node(name = name, args = normalized_args, kwargs = normalized_kwargs)
  )
}

normalize_op_step <- function(op, current, idx, vars = NULL) {
  assert_named_list(op, sprintf("op step %d", idx))

  type <- op$type
  if (!is_scalar_string(type)) {
    fail(sprintf("op step %d `type` must be a single string", idx))
  }
  if (!(type %in% c("add", "sub", "mul", "div"))) {
    fail(sprintf("op step %d `type` must be one of add, sub, mul, div", idx))
  }
  if (is.null(op$right)) {
    fail(sprintf("op step %d must define `right`", idx))
  }

  new_binary_op_node(
    op = type,
    left = current,
    right = normalize_step_arg(op$right, vars = vars)
  )
}
