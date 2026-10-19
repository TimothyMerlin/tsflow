#' Read a Pipeline Definition
#'
#' Reads a tsflow pipeline file into an unnormalized R object. YAML files are
#' read directly. CSV files are treated as wide mapping sheets with columns
#' `name,input,step_1,...`, then compiled into the same structured `steps`
#' representation used by YAML.
#'
#' CSV step cells are R function calls such as `foo(bar)` or
#' `jsa(raw_b, method = 'uniform')`, parsed with R's own parser. `add`, `sub`,
#' `mul` and `div` with a single argument are arithmetic op steps. Arguments
#' can be:
#'
#' - series names (`raw_b`, `gdp.nominal.quarterly`),
#' - numbers (`100`, `-1`, `1e6`), including arithmetic on numbers such as
#'   `100 / 4`, which is evaluated while reading,
#' - quoted strings (`'uniform'` or `"uniform"`) and `TRUE` / `FALSE`,
#' - sidecar var references `@name` (or `.var(name)`),
#' - nested calls such as `a2q_sum(jsa(raw_b), conversion = @conversion)`,
#'   including namespaced functions like `stats::lag(raw_b, -1)`.
#'
#' If a sibling `.meta.yml` or `.meta.yaml` file exists, `read_pipeline()`
#' merges its `vars`, `outputs`, and `metadata` into the parsed pipeline.
#'
#' @param path Path to a pipeline file. YAML and CSV are supported.
#'
#' @return An unnormalized R list in the pipeline schema expected by
#'   `normalize_pipeline()`.
#' @export
read_pipeline <- function(path) {
  if (!is_scalar_string(path)) {
    fail("`path` must be a single string")
  }

  extension <- tolower(tools::file_ext(path))
  parsed <- if (identical(extension, "csv")) {
    read_pipeline_csv(path)
  } else {
    yaml::read_yaml(path)
  }

  merge_pipeline_sidecar(parsed, path)
}

#' Write a Pipeline Definition
#'
#' Writes an in-memory tsflow pipeline definition to YAML on disk. This is
#' useful for persisting the canonical parsed pipeline shape produced by
#' `read_pipeline()`, including pipelines originally authored as CSV plus a
#' sibling sidecar.
#'
#' @param x A pipeline definition as an R list.
#' @param path Output path. Should typically end in `.yml` or `.yaml`.
#'
#' @return Invisibly returns `path`.
#' @export
write_pipeline <- function(x, path) {
  if (!is.list(x)) {
    fail("`x` must be a pipeline definition list")
  }
  if (!is_scalar_string(path)) {
    fail("`path` must be a single string")
  }

  yaml_text <- yaml::as.yaml(x)
  writeLines(yaml_text, con = path, useBytes = TRUE)

  invisible(path)
}

#' @rdname read_pipeline
#' @export
read_pipeline_csv <- function(path) {
  if (!is_scalar_string(path)) {
    fail("`path` must be a single string")
  }

  data <- utils::read.csv(
    file = path,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  required_columns <- c("name", "input")
  missing_columns <- setdiff(required_columns, names(data))
  if (length(missing_columns) > 0L) {
    fail(sprintf(
      "CSV pipeline is missing required columns: %s",
      paste(missing_columns, collapse = ", ")
    ))
  }

  input_index <- match("input", names(data))
  step_columns <- if (input_index < ncol(data)) names(data)[seq.int(input_index + 1L, ncol(data))] else character()
  target_names <- vapply(seq_len(nrow(data)), function(idx) trim_cell(data$name[[idx]]), character(1))

  derived_series <- vector("list", nrow(data))
  external_refs <- character()

  for (idx in seq_len(nrow(data))) {
    name <- target_names[[idx]]
    input <- trim_cell(data$input[[idx]])

    if (!is_scalar_string(name) || !nzchar(name)) {
      fail(sprintf("CSV row %d must define a non-empty `name`", idx))
    }
    if (!is_scalar_string(input) || !nzchar(input)) {
      fail(sprintf("CSV row %d must define a non-empty `input`", idx))
    }

    steps <- list(list(input = input))
    external_refs <- c(external_refs, input)

    if (length(step_columns) > 0L) {
      for (column in step_columns) {
        cell <- trim_cell(data[[column]][[idx]])
        if (!is_scalar_string(cell) || !nzchar(cell)) {
          next
        }

        parsed_step <- parse_csv_step(cell, input_name = input)
        steps[[length(steps) + 1L]] <- parsed_step$step
        external_refs <- c(external_refs, parsed_step$refs)
      }
    }

    derived_series[[idx]] <- list(
      name = name,
      steps = steps
    )
  }

  raw_refs <- unique(external_refs[!(external_refs %in% target_names)])
  raw_series <- lapply(raw_refs, function(name) {
    list(
      name = name,
      source = list(type = "injected")
    )
  })

  list(series = c(raw_series, derived_series))
}

merge_pipeline_sidecar <- function(parsed, path) {
  sidecar_path <- find_pipeline_sidecar(path)
  if (is.null(sidecar_path)) {
    return(parsed)
  }

  sidecar <- yaml::read_yaml(sidecar_path)
  assert_named_list(sidecar, "pipeline sidecar")

  allowed_keys <- c("vars", "outputs", "metadata")
  unknown_keys <- setdiff(names(sidecar), allowed_keys)
  if (length(unknown_keys) > 0L) {
    fail(sprintf(
      "pipeline sidecar contains unsupported keys: %s",
      paste(unknown_keys, collapse = ", ")
    ))
  }

  for (key in names(sidecar)) {
    if (!is.null(parsed[[key]])) {
      fail(sprintf("pipeline and sidecar both define `%s`", key))
    }
    parsed[[key]] <- sidecar[[key]]
  }

  parsed
}

find_pipeline_sidecar <- function(path) {
  stem <- sub("\\.[^.]+$", "", path)
  candidates <- c(
    paste0(stem, ".meta.yml"),
    paste0(stem, ".meta.yaml")
  )

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0L) {
    return(NULL)
  }
  if (length(existing) > 1L) {
    fail(sprintf(
      "multiple pipeline sidecars found for `%s`: %s",
      basename(path),
      paste(basename(existing), collapse = ", ")
    ))
  }

  existing[[1L]]
}

parse_csv_step <- function(cell, input_name = NULL) {
  expr <- parse_csv_cell(cell)
  name <- csv_call_name(expr)

  if (is.null(name) || name %in% c(csv_arithmetic_ops, ".var")) {
    fail(sprintf(
      "CSV step `%s` must use the form `fn(arg1, ...)` or `op(arg)`",
      cell
    ))
  }

  args <- as.list(expr)[-1L]
  assert_no_empty_csv_args(args, cell)

  if (identical(name, "fct_chain")) {
    return(parse_csv_fct_chain_step(
      args = args,
      cell = cell,
      input_name = input_name
    ))
  }

  if (identical(name, "fct") && length(args) > 0L && is_csv_arithmetic_call(args[[1L]])) {
    return(parse_csv_formula_step(
      name = name,
      formula_text = deparse_csv_expr(args[[1L]]),
      cell = cell,
      input_name = input_name
    ))
  }

  converted <- convert_csv_args(args, cell)

  if (name %in% c("add", "sub", "mul", "div")) {
    if (length(converted$kwargs) > 0L) {
      fail(sprintf("CSV op step `%s` does not support named arguments", cell))
    }
    if (length(converted$args) != 1L) {
      fail(sprintf("CSV op step `%s` must define exactly one argument", cell))
    }

    return(list(
      step = list(
        op = list(
          type = name,
          right = converted$args[[1L]]
        )
      ),
      refs = converted$refs
    ))
  }

  list(
    step = list(
      call = list(
        name = name,
        args = converted$args,
        kwargs = converted$kwargs
      )
    ),
    refs = converted$refs
  )
}

csv_arithmetic_ops <- c("+", "-", "*", "/")

# Parse one CSV step cell with R's parser. `@name` var references are not
# valid R, so they are rewritten to `.var(name)` first, outside quoted strings
# and only where `@` starts a token.
parse_csv_cell <- function(cell) {
  quoted_pattern <- "\"[^\"]*\"|'[^']*'"
  quoted <- regmatches(cell, gregexpr(quoted_pattern, cell))[[1L]]
  unquoted <- regmatches(cell, gregexpr(quoted_pattern, cell), invert = TRUE)[[1L]]
  unquoted <- gsub(
    "(?<![A-Za-z0-9_.])@([A-Za-z_][A-Za-z0-9_.]*)",
    ".var(\\1)",
    unquoted,
    perl = TRUE
  )
  rewritten <- paste0(c(rbind(unquoted, c(quoted, ""))), collapse = "")

  tryCatch(
    str2lang(rewritten),
    error = function(e) {
      fail(sprintf(
        "CSV step `%s` is not a valid expression: %s",
        cell,
        conditionMessage(e)
      ))
    }
  )
}

# Function name of a call as a string (`fn` or `pkg::fn`), the arithmetic
# operator of an infix call, or NULL if `expr` is not a call with a plain or
# namespaced function name (e.g. `x[1]`, `x$y`, `x@y`, `function(x) x`).
csv_call_name <- function(expr) {
  if (!is.call(expr)) {
    return(NULL)
  }

  head <- expr[[1L]]
  if (is.symbol(head)) {
    name <- as.character(head)
    if (name %in% csv_arithmetic_ops || is_syntactic_name(name)) {
      return(name)
    }
    return(NULL)
  }

  if (is.call(head) && identical(head[[1L]], as.symbol("::")) &&
      is.symbol(head[[2L]]) && is.symbol(head[[3L]]) &&
      is_syntactic_name(as.character(head[[3L]]))) {
    return(paste0(as.character(head[[2L]]), "::", as.character(head[[3L]])))
  }

  NULL
}

is_syntactic_name <- function(name) {
  identical(make.names(name), name)
}

is_csv_arithmetic_call <- function(expr) {
  is.call(expr) && is.symbol(expr[[1L]]) && as.character(expr[[1L]]) %in% csv_arithmetic_ops
}

is_missing_csv_arg <- function(expr) {
  is.symbol(expr) && identical(as.character(expr), "")
}

assert_no_empty_csv_args <- function(args, cell) {
  if (any(vapply(args, is_missing_csv_arg, logical(1)))) {
    fail(sprintf("CSV step `%s` contains an empty argument", cell))
  }
}

deparse_csv_expr <- function(expr) {
  paste(deparse(expr, width.cutoff = 500L), collapse = "")
}

# Convert parsed call arguments into YAML-shaped step values, splitting
# positional args from named kwargs and collecting referenced series names.
convert_csv_args <- function(args, cell) {
  arg_names <- names(args) %||% rep("", length(args))
  positional <- !nzchar(arg_names)

  converted <- lapply(args, convert_csv_arg, cell = cell)
  values <- lapply(converted, `[[`, "value")

  list(
    args = unname(values[positional]),
    kwargs = values[!positional],
    refs = unlist(
      c(lapply(converted[positional], `[[`, "refs"), lapply(converted[!positional], `[[`, "refs")),
      use.names = FALSE
    ) %||% character()
  )
}

convert_csv_arg <- function(expr, cell) {
  if (is.symbol(expr)) {
    name <- as.character(expr)
    return(list(value = name, refs = name))
  }

  if (is.character(expr) && length(expr) == 1L) {
    return(list(value = list(literal = expr), refs = character()))
  }

  if ((is.numeric(expr) || is.logical(expr)) && length(expr) == 1L) {
    if (is.na(expr)) {
      fail(sprintf("CSV step `%s` contains a missing value (`NA`), which is not supported", cell))
    }
    return(list(value = expr, refs = character()))
  }

  if (is.null(expr)) {
    fail(sprintf("CSV step `%s` contains `NULL`, which is not supported as an argument", cell))
  }

  if (is_csv_arithmetic_call(expr)) {
    return(list(value = fold_csv_number(expr, cell), refs = character()))
  }

  if (is.call(expr) && identical(expr[[1L]], as.symbol("("))) {
    return(convert_csv_arg(expr[[2L]], cell))
  }

  name <- csv_call_name(expr)

  if (identical(name, ".var")) {
    if (length(expr) != 2L || !(is.symbol(expr[[2L]]) || is_scalar_string(expr[[2L]]))) {
      fail(sprintf("CSV step `%s` must use `.var(name)` with a single var name", cell))
    }
    return(list(value = list(var = as.character(expr[[2L]])), refs = character()))
  }

  if (!is.null(name) && !(name %in% c(csv_arithmetic_ops, "add", "sub", "mul", "div", "fct", "fct_chain"))) {
    args <- as.list(expr)[-1L]
    assert_no_empty_csv_args(args, cell)
    nested <- convert_csv_args(args, cell)
    return(list(
      value = list(call = list(name = name, args = nested$args, kwargs = nested$kwargs)),
      refs = nested$refs
    ))
  }

  fail(sprintf(
    "CSV step `%s` contains an unsupported argument `%s`; arguments must be series names, numbers, quoted strings, `TRUE`/`FALSE`, `@var` references or nested calls",
    cell,
    deparse_csv_expr(expr)
  ))
}

# Evaluate arithmetic on number literals, e.g. `100 / 3` or `-1`, while reading.
fold_csv_number <- function(expr, cell) {
  if (is.numeric(expr) && length(expr) == 1L && !is.na(expr)) {
    return(expr)
  }

  if (is.call(expr) && identical(expr[[1L]], as.symbol("("))) {
    return(fold_csv_number(expr[[2L]], cell))
  }

  if (is_csv_arithmetic_call(expr)) {
    op <- get(as.character(expr[[1L]]), envir = baseenv())
    operands <- lapply(as.list(expr)[-1L], fold_csv_number, cell = cell)
    return(do.call(op, operands))
  }

  fail(sprintf(
    "CSV step `%s` uses arithmetic on `%s`; arithmetic inside arguments only supports numbers",
    cell,
    deparse_csv_expr(expr)
  ))
}

parse_csv_fct_chain_step <- function(args, cell, input_name) {
  if (length(args) < 2L) {
    fail(sprintf(
      "CSV fct_chain step `%s` must use `fct_chain(real_formula, nom=nominal_formula, ...)`",
      cell
    ))
  }

  arg_names <- names(args) %||% rep("", length(args))
  if (nzchar(arg_names[[1L]])) {
    fail(sprintf(
      "CSV fct_chain step `%s` must use `fct_chain(real_formula, nom=nominal_formula, ...)`",
      cell
    ))
  }
  if (!identical(arg_names[[2L]], "nom")) {
    fail(sprintf(
      "CSV fct_chain step `%s` must define the nominal formula as `nom=...`",
      cell
    ))
  }

  real_parsed <- parse_csv_formula(deparse_csv_expr(args[[1L]]), cell)
  nominal_parsed <- parse_csv_formula(deparse_csv_expr(args[[2L]]), cell)

  if (!is_scalar_string(input_name) || !nzchar(input_name)) {
    fail(sprintf("CSV formula step `%s` requires a non-empty row `input`", cell))
  }
  if (!identical(real_parsed$tokens[[1L]], input_name)) {
    fail(sprintf(
      "CSV formula step `%s` must start with the row input `%s`",
      cell,
      input_name
    ))
  }
  if (any(real_parsed$ops[-1L] %in% c("*", "/")) ||
      any(nominal_parsed$ops[-1L] %in% c("*", "/"))) {
    fail(sprintf(
      "CSV fct_chain step `%s` only supports `+` and `-` operators",
      cell
    ))
  }

  kwargs <- list(
    formula = list(literal = real_parsed$formula),
    tokens = list(literal = paste(real_parsed$tokens, collapse = ",")),
    ops = list(literal = paste(real_parsed$ops, collapse = ",")),
    nominal_formula = list(literal = nominal_parsed$formula),
    nominal_tokens = list(literal = paste(nominal_parsed$tokens, collapse = ",")),
    nominal_ops = list(literal = paste(nominal_parsed$ops, collapse = ",")),
    input_name = list(literal = input_name)
  )

  for (token in unique(real_parsed$tokens[-1L])) {
    kwargs[[paste0("ref__", token)]] <- token
  }
  for (token in unique(nominal_parsed$tokens)) {
    kwargs[[paste0("nominal__", token)]] <- token
  }

  if (length(args) > 2L) {
    extra_args <- convert_csv_args(args[-c(1L, 2L)], cell)
    if (length(extra_args$args) > 0L) {
      fail(sprintf(
        "CSV fct_chain step `%s` only supports additional named arguments after `nom=...`",
        cell
      ))
    }
    kwargs <- c(kwargs, extra_args$kwargs)
  }

  list(
    step = list(
      call = list(
        name = "fct_chain_formula",
        kwargs = kwargs
      )
    ),
    refs = unique(c(real_parsed$tokens[-1L], nominal_parsed$tokens))
  )
}

parse_csv_formula_step <- function(name, formula_text, cell, input_name) {
  if (!is_scalar_string(input_name) || !nzchar(input_name)) {
    fail(sprintf("CSV formula step `%s` requires a non-empty row `input`", cell))
  }

  parsed <- parse_csv_formula(formula_text, cell)
  if (!identical(parsed$tokens[[1L]], input_name)) {
    fail(sprintf(
      "CSV formula step `%s` must start with the row input `%s`",
      cell,
      input_name
    ))
  }

  kwargs <- list(
    formula = list(literal = parsed$formula),
    tokens = list(literal = paste(parsed$tokens, collapse = ",")),
    ops = list(literal = paste(parsed$ops, collapse = ",")),
    input_name = list(literal = input_name)
  )

  extra_tokens <- parsed$tokens[-1L]
  for (token in extra_tokens) {
    kwargs[[paste0("ref__", token)]] <- token
  }

  list(
    step = list(
      call = list(
        name = paste0(name, "_formula"),
        kwargs = kwargs
      )
    ),
    refs = unique(unname(unlist(kwargs[vapply(kwargs, is.character, logical(1))], use.names = FALSE)))
  )
}

parse_csv_formula <- function(text, cell) {
  formula <- gsub("\\s+", "", text)

  if (!nzchar(formula)) {
    fail(sprintf("CSV formula step `%s` must define a non-empty formula", cell))
  }
  if (grepl("[(),=@'\"]", formula)) {
    fail(sprintf(
      "CSV formula step `%s` only supports flat infix formulas over series refs",
      cell
    ))
  }

  tokens <- strsplit(formula, "[+\\-*/]", perl = TRUE)[[1L]]
  ops_match <- gregexpr("[+\\-*/]", formula, perl = TRUE)
  ops <- regmatches(formula, ops_match)[[1L]]

  if (length(tokens) == 0L || any(!nzchar(tokens))) {
    fail(sprintf("CSV formula step `%s` has an invalid operator sequence", cell))
  }

  invalid_tokens <- !grepl("^[A-Za-z_][A-Za-z0-9_.]*$", tokens)
  if (any(invalid_tokens)) {
    fail(sprintf(
      "CSV formula step `%s` contains invalid operands: %s",
      cell,
      paste(tokens[invalid_tokens], collapse = ", ")
    ))
  }

  list(
    formula = formula,
    tokens = tokens,
    ops = c("+", ops)
  )
}

trim_character <- function(x) {
  trimws(as.character(x))
}

trim_cell <- function(x) {
  value <- trim_character(x)
  if (identical(value, "NA")) {
    return("")
  }
  value
}
