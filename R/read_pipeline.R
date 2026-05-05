#' Read a Pipeline Definition
#'
#' Reads a tsflow pipeline file into an unnormalized R object. YAML files are
#' read directly. CSV files are treated as wide mapping sheets with columns
#' `name,input,step_1,...`, then compiled into the same structured `steps`
#' representation used by YAML.
#'
#' CSV step cells support function-style calls such as `foo(bar)` or
#' `jsa(raw_b, method = 'uniform')`, with `add/sub/mul/div(...)` treated as
#' arithmetic op steps. If a sibling `.meta.yml` or `.meta.yaml` file exists,
#' `read_pipeline()` merges its `vars`, `outputs`, and `metadata` into the
#' parsed pipeline. CSV step cells can reference sidecar vars with `@name`.
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
  match <- regexec("^\\s*([^()]+?)\\s*\\((.*)\\)\\s*$", cell)
  captures <- regmatches(cell, match)[[1L]]

  if (length(captures) == 0L) {
    fail(sprintf(
      "CSV step `%s` must use the form `fn(arg1, ...)` or `op(arg)`",
      cell
    ))
  }

  name <- trimws(captures[[2L]])
  args_text <- trimws(captures[[3L]])

  if (!nzchar(name)) {
    fail(sprintf("CSV step `%s` must define a non-empty function or op name", cell))
  }

  if (identical(name, "fct_chain")) {
    return(parse_csv_fct_chain_step(
      args_text = args_text,
      cell = cell,
      input_name = input_name
    ))
  }

  if (identical(name, "fct") && grepl("[+*/-]", args_text, perl = TRUE)) {
    return(parse_csv_formula_step(
      name = name,
      formula_text = args_text,
      cell = cell,
      input_name = input_name
    ))
  }

  parsed_args <- parse_csv_step_args(args_text, cell)
  refs <- c(
    vapply(parsed_args$args[vapply(parsed_args$args, inherits, logical(1), "RefNode")], `[[`, character(1), "name"),
    vapply(parsed_args$kwargs[vapply(parsed_args$kwargs, inherits, logical(1), "RefNode")], `[[`, character(1), "name")
  )

  if (name %in% c("add", "sub", "mul", "div")) {
    if (length(parsed_args$kwargs) > 0L) {
      fail(sprintf("CSV op step `%s` does not support named arguments", cell))
    }
    if (length(parsed_args$args) != 1L) {
      fail(sprintf("CSV op step `%s` must define exactly one argument", cell))
    }

    return(list(
      step = list(
        op = list(
          type = name,
          right = csv_arg_to_step_value(parsed_args$args[[1L]])
        )
      ),
      refs = refs
    ))
  }

  return(list(
    step = list(
      call = list(
        name = name,
        args = lapply(parsed_args$args, csv_arg_to_step_value),
        kwargs = lapply(parsed_args$kwargs, csv_arg_to_step_value)
      )
    ),
    refs = refs
  ))
}

parse_csv_fct_chain_step <- function(args_text, cell, input_name) {
  parts <- split_csv_arguments(args_text, cell)
  if (length(parts) < 2L) {
    fail(sprintf(
      "CSV fct_chain step `%s` must use `fct_chain(real_formula, nom=nominal_formula, ...)`",
      cell
    ))
  }

  real_formula <- trimws(parts[[1L]])
  nominal_part <- trimws(parts[[2L]])
  nominal_match <- regexec("^nom\\s*=\\s*(.+)$", nominal_part)
  nominal_captures <- regmatches(nominal_part, nominal_match)[[1L]]
  if (length(nominal_captures) == 0L) {
    fail(sprintf(
      "CSV fct_chain step `%s` must define the nominal formula as `nom=...`",
      cell
    ))
  }

  real_parsed <- parse_csv_formula(real_formula, cell)
  nominal_parsed <- parse_csv_formula(trimws(nominal_captures[[2L]]), cell)

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

  if (length(parts) > 2L) {
    extra_args <- parse_csv_step_args(paste(parts[-c(1L, 2L)], collapse = ", "), cell)
    if (length(extra_args$args) > 0L) {
      fail(sprintf(
        "CSV fct_chain step `%s` only supports additional named arguments after `nom=...`",
        cell
      ))
    }
    kwargs <- c(kwargs, lapply(extra_args$kwargs, csv_arg_to_step_value))
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

parse_csv_step_args <- function(text, cell) {
  if (!nzchar(text)) {
    return(list(args = list(), kwargs = list()))
  }

  pieces <- split_csv_arguments(text, cell)
  if (any(!nzchar(pieces))) {
    fail(sprintf("CSV step `%s` contains an empty argument", cell))
  }

  args <- list()
  kwargs <- list()

  for (piece in pieces) {
    parsed <- parse_csv_arg(piece)
    if (parsed$type == "kwarg") {
      kwargs[[parsed$name]] <- parsed$value
    } else {
      args[[length(args) + 1L]] <- parsed$value
    }
  }

  list(args = args, kwargs = kwargs)
}

parse_csv_arg <- function(x) {
  value <- trimws(x)

  kwarg_match <- regexec("^([A-Za-z_][A-Za-z0-9_.-]*)\\s*=\\s*(.+)$", value)
  kwarg_captures <- regmatches(value, kwarg_match)[[1L]]
  if (length(kwarg_captures) > 0L) {
    return(list(
      type = "kwarg",
      name = kwarg_captures[[2L]],
      value = parse_csv_scalar(trimws(kwarg_captures[[3L]]))
    ))
  }

  list(
    type = "arg",
    value = parse_csv_scalar(value)
  )
}

parse_csv_scalar <- function(value) {
  if (grepl("^[+-]?[0-9]+(\\.[0-9]+)?$", value)) {
    return(new_literal_node(as.numeric(value)))
  }
  if (identical(value, "TRUE")) {
    return(new_literal_node(TRUE))
  }
  if (identical(value, "FALSE")) {
    return(new_literal_node(FALSE))
  }
  if (grepl("^'.*'$", value) || grepl('^".*"$', value)) {
    return(new_literal_node(substr(value, 2L, nchar(value) - 1L)))
  }
  if (grepl("^@[A-Za-z_][A-Za-z0-9_.-]*$", value)) {
    return(new_var_placeholder(sub("^@", "", value)))
  }
  if (grepl("[()]", value)) {
    fail(sprintf(
      "CSV argument `%s` is invalid; nested calls and expressions are not supported",
      value
    ))
  }
  if (!nzchar(value)) {
    fail("CSV arguments must be non-empty")
  }

  new_ref_node(value)
}

csv_arg_to_step_value <- function(arg) {
  if (inherits(arg, "RefNode")) {
    return(arg$name)
  }

  if (inherits(arg, "LiteralNode")) {
    if (is_scalar_string(arg$value)) {
      return(list(literal = arg$value))
    }
    return(arg$value)
  }

  if (inherits(arg, "VarPlaceholder")) {
    return(list(var = arg$name))
  }

  fail("CSV step arguments must normalize to refs or literals")
}

new_var_placeholder <- function(name) {
  structure(
    list(name = name),
    class = c("VarPlaceholder", "tsflow_spec")
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

split_csv_arguments <- function(text, cell) {
  chars <- strsplit(text, "", fixed = TRUE)[[1L]]
  pieces <- character()
  current <- character()
  quote_char <- NULL

  for (char in chars) {
    if (is.null(quote_char)) {
      if (char %in% c("'", "\"")) {
        quote_char <- char
        current <- c(current, char)
        next
      }
      if (char == ",") {
        pieces <- c(pieces, trimws(paste(current, collapse = "")))
        current <- character()
        next
      }
    } else if (identical(char, quote_char)) {
      quote_char <- NULL
    }

    current <- c(current, char)
  }

  if (!is.null(quote_char)) {
    fail(sprintf("CSV step `%s` contains an unterminated quoted string", cell))
  }

  c(pieces, trimws(paste(current, collapse = "")))
}
