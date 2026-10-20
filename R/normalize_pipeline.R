#' Normalize Pipeline Structures
#'
#' Converts parsed YAML or CSV-derived structures into the canonical tsflow
#' internal model.
#'
#' The MVP normalizer supports raw series defined by `source` and derived series
#' defined by structured `steps`. Derived expressions are normalized into
#' reference, literal, call, pipe, and binary operator nodes.
#'
#' Step arguments may be a series name (a bare string, or a map with a `ref`
#' entry), a number or flag, a map with a `literal` or `var` entry, or a
#' nested call: a map with a `call` entry holding `name`, `args` and `kwargs`.
#'
#' Instead of `steps`, a derived series can be written as a single `expr`
#' string using the CSV cell syntax, e.g.
#' `expr: "jsa(x, method = 'uniform') / 1000"`. As with R's pipe, the first
#' argument of each call is the piped input, so this is equivalent to the steps
#' `input: x`, `call: jsa(method = 'uniform')`, `op: div 1000`. The innermost
#' first argument must be a series.
#'
#' The right side of `+ - * /` can be a series, number, var or call, but not a
#' calculation on series: `a / (b + c)` is rejected. Define the calculation as
#' its own series and reference it instead, e.g. a series `b_plus_c` with
#' `expr: "b + c"` and then `expr: "a / b_plus_c"`. The left side has no such
#' limit, so `(a + b) / c` works.
#'
#' @param x A parsed pipeline, as returned by [read_pipeline()].
#'
#' @return A `PipelineSpec` object.
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

normalize_series <- function(x, vars = NULL) {
  assert_named_list(x, "series entry")

  name <- x$name
  if (!is_scalar_string(name)) {
    fail("series entry `name` must be a single string")
  }

  definitions <- c(source = !is.null(x$source), steps = !is.null(x$steps), expr = !is.null(x$expr))
  has_source <- definitions[["source"]]

  if (sum(definitions) > 1L) {
    fail(sprintf(
      "series `%s` cannot define both %s",
      name,
      paste(sprintf("`%s`", names(definitions)[definitions]), collapse = " and ")
    ))
  }
  if (!any(definitions)) {
    fail(sprintf("series `%s` must define either `source`, `steps` or `expr`", name))
  }

  steps <- x$steps
  if (definitions[["expr"]]) {
    if (!is_scalar_string(x$expr)) {
      fail(sprintf("series `%s` `expr` must be a single string", name))
    }
    steps <- expr_to_steps(x$expr, name)
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
      expression = normalize_steps(steps, vars = vars),
      description = description,
      metadata = metadata
    )
  }
}

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

  if (is.list(x) && identical(names(x), "call")) {
    return(normalize_call(x$call, sprintf("nested call `%s`", x$call$name %||% "?"), vars = vars))
  }

  fail("step args must normalize to a literal value, series reference, var or nested call")
}

# Convert an `expr:` string into the equivalent `steps` list, so an expr
# series is indistinguishable from one written as steps. As with R's pipe,
# the first argument of a call is the piped input: `jsa(x, method = 'u') /
# 1000` becomes `input: x`, `call: jsa(method = 'u')`, `op: div 1000`.
expr_to_steps <- function(text, series_name) {
  context <- sprintf("series `%s` `expr`", series_name)
  expr_chain_steps(parse_step_text(text, context), context)
}

expr_chain_steps <- function(expr, context) {
  if (is.symbol(expr) && !is_missing_csv_arg(expr)) {
    return(list(list(input = as.character(expr))))
  }

  if (is.call(expr) && identical(expr[[1L]], as.symbol("("))) {
    return(expr_chain_steps(expr[[2L]], context))
  }

  name <- csv_call_name(expr)
  op_types <- c("+" = "add", "-" = "sub", "*" = "mul", "/" = "div")

  if (!is.null(name) && name %in% names(op_types)) {
    if (length(expr) != 3L) {
      fail(sprintf(
        "%s uses unary `%s` on `%s`; write it as arithmetic between a series and a value, e.g. `x * -1`",
        context,
        name,
        deparse_csv_expr(expr[[2L]])
      ))
    }
    return(c(
      expr_chain_steps(expr[[2L]], context),
      list(list(op = list(type = op_types[[name]], right = expr_operand(expr[[3L]], name, context))))
    ))
  }

  if (!is.null(name) && name %in% c("fct", "fct_chain")) {
    fail(sprintf("%s uses `%s()`; `fct` / `fct_chain` formulas are only supported in CSV steps", context, name))
  }

  if (!is.null(name) && !identical(name, ".var")) {
    args <- as.list(expr)[-1L]
    assert_no_empty_csv_args(args, context)
    arg_names <- names(args) %||% rep("", length(args))

    if (length(args) == 0L || nzchar(arg_names[[1L]])) {
      fail(sprintf(
        "%s: `%s()` needs the series it applies to as its first, unnamed argument",
        context,
        name
      ))
    }

    input_steps <- expr_chain_steps(args[[1L]], context)
    rest <- convert_csv_args(args[-1L], context)

    if (name %in% op_types) {
      if (length(rest$kwargs) > 0L || length(rest$args) != 1L) {
        fail(sprintf("%s: `%s()` takes a series and exactly one unnamed operand", context, name))
      }
      return(c(input_steps, list(list(op = list(type = name, right = rest$args[[1L]])))))
    }

    return(c(input_steps, list(list(call = list(name = name, args = rest$args, kwargs = rest$kwargs)))))
  }

  fail(sprintf(
    "%s must start from a series, but `%s` is not a series name; the innermost first argument must be a series",
    context,
    deparse_csv_expr(expr)
  ))
}

# Right-hand operand of an arithmetic step: a series, value, var or call, but
# not a calculation on series such as `b + c` (that needs its own series).
# Deliberately unsupported for now (#22).
expr_operand <- function(expr, op, context) {
  while (is.call(expr) && identical(expr[[1L]], as.symbol("("))) {
    expr <- expr[[2L]]
  }

  if (is_csv_arithmetic_call(expr) && !is_number_expr(expr)) {
    fail(sprintf(
      "%s: the right side of `%s` (`%s`) is a calculation on series; define it as its own series and reference that instead",
      context,
      op,
      deparse_csv_expr(expr)
    ))
  }

  convert_csv_arg(expr, context)$value
}

is_number_expr <- function(expr) {
  if (is.numeric(expr) && length(expr) == 1L && !is.na(expr)) {
    return(TRUE)
  }
  if (is.call(expr) && identical(expr[[1L]], as.symbol("("))) {
    return(is_number_expr(expr[[2L]]))
  }
  if (is_csv_arithmetic_call(expr)) {
    return(all(vapply(as.list(expr)[-1L], is_number_expr, logical(1))))
  }
  FALSE
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
  new_pipe_node(
    input = current,
    call = normalize_call(call, sprintf("call step %d", idx), vars = vars)
  )
}

# Shared by pipe `call` steps and nested `{call: ...}` step args.
normalize_call <- function(call, what, vars = NULL) {
  assert_named_list(call, what)

  name <- call$name
  if (!is_scalar_string(name)) {
    fail(sprintf("%s `name` must be a single string", what))
  }

  args <- call$args %||% list()
  if (!is.list(args)) {
    args <- list(args)
  }
  normalized_args <- lapply(args, normalize_step_arg, vars = vars)

  kwargs <- call$kwargs %||% list()
  if (!is.list(kwargs)) {
    fail(sprintf("%s `kwargs` must be a named list", what))
  }
  if (length(kwargs) > 0L && is.null(names(kwargs))) {
    fail(sprintf("%s `kwargs` must be a named list", what))
  }
  normalized_kwargs <- lapply(kwargs, normalize_step_arg, vars = vars)

  new_call_node(name = name, args = normalized_args, kwargs = normalized_kwargs)
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
