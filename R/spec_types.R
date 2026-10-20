new_pipeline_spec <- function(series, outputs = NULL, metadata = NULL, vars = NULL) {
  structure(
    list(
      series = series,
      outputs = outputs,
      metadata = metadata,
      vars = vars
    ),
    class = c("PipelineSpec", "tsflow_spec")
  )
}

new_series_spec <- function(name,
                            kind,
                            source = NULL,
                            expression = NULL,
                            description = NULL,
                            metadata = NULL) {
  structure(
    list(
      name = name,
      kind = kind,
      source = source,
      expression = expression,
      description = description,
      metadata = metadata
    ),
    class = c("SeriesSpec", "tsflow_spec")
  )
}

new_source_spec <- function(type, config = list()) {
  structure(
    list(
      type = type,
      config = config
    ),
    class = c("SourceSpec", "tsflow_spec")
  )
}

new_ref_node <- function(name) {
  structure(
    list(name = name),
    class = c("RefNode", "ExprNode", "tsflow_spec")
  )
}

# `var` records the shared parameter a value came from, for documentation.
new_literal_node <- function(value, var = NULL) {
  node <- list(value = value)
  if (!is.null(var)) {
    node$var <- var
  }
  structure(node, class = c("LiteralNode", "ExprNode", "tsflow_spec"))
}

new_call_node <- function(name, args = list(), kwargs = list()) {
  structure(
    list(
      name = name,
      args = args,
      kwargs = kwargs
    ),
    class = c("CallNode", "ExprNode", "tsflow_spec")
  )
}

new_pipe_node <- function(input, call) {
  structure(
    list(
      input = input,
      call = call
    ),
    class = c("PipeNode", "ExprNode", "tsflow_spec")
  )
}

new_binary_op_node <- function(op, left, right) {
  structure(
    list(
      op = op,
      left = left,
      right = right
    ),
    class = c("BinaryOpNode", "ExprNode", "tsflow_spec")
  )
}
