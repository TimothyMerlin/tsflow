#' Extract Series Dependencies
#'
#' Walks a normalized expression tree and returns the referenced upstream
#' series.
#'
#' @param expr A normalized expression node.
#'
#' @return A character vector of referenced series names.
#' @export
extract_dependencies <- function(expr) {
  if (inherits(expr, "RefNode")) {
    return(expr$name)
  }

  if (inherits(expr, "LiteralNode")) {
    return(character())
  }

  if (inherits(expr, "CallNode")) {
    deps <- c(
      unlist(lapply(expr$args, extract_dependencies), use.names = FALSE),
      unlist(lapply(expr$kwargs, extract_dependencies), use.names = FALSE)
    )
    return(unique(deps))
  }

  if (inherits(expr, "PipeNode")) {
    deps <- c(
      extract_dependencies(expr$input),
      extract_dependencies(expr$call)
    )
    return(unique(deps))
  }

  if (inherits(expr, "BinaryOpNode")) {
    deps <- c(
      extract_dependencies(expr$left),
      extract_dependencies(expr$right)
    )
    return(unique(deps))
  }

  fail(sprintf(
    "cannot extract dependencies from unsupported expression class `%s`",
    paste(class(expr), collapse = ", ")
  ))
}

# Series referenced from call args/kwargs or op operands, as opposed to the
# `input` a step chain starts from.
extract_arg_dependencies <- function(expr) {
  if (inherits(expr, c("RefNode", "LiteralNode"))) {
    return(character())
  }

  if (inherits(expr, "CallNode")) {
    return(unique(extract_dependencies(expr)))
  }

  if (inherits(expr, "PipeNode")) {
    return(unique(c(
      extract_arg_dependencies(expr$input),
      extract_dependencies(expr$call)
    )))
  }

  if (inherits(expr, "BinaryOpNode")) {
    return(unique(c(
      extract_arg_dependencies(expr$left),
      extract_dependencies(expr$right)
    )))
  }

  character()
}

collect_transform_names <- function(expr) {
  if (inherits(expr, c("RefNode", "LiteralNode"))) {
    return(character())
  }

  if (inherits(expr, "CallNode")) {
    nested <- c(
      unlist(lapply(expr$args, collect_transform_names), use.names = FALSE),
      unlist(lapply(expr$kwargs, collect_transform_names), use.names = FALSE)
    )
    return(unique(c(expr$name, nested)))
  }

  if (inherits(expr, "PipeNode")) {
    names <- c(
      collect_transform_names(expr$input),
      collect_transform_names(expr$call)
    )
    return(unique(names))
  }

  if (inherits(expr, "BinaryOpNode")) {
    names <- c(
      collect_transform_names(expr$left),
      collect_transform_names(expr$right)
    )
    return(unique(names))
  }

  fail(sprintf(
    "cannot collect transform names from unsupported expression class `%s`",
    paste(class(expr), collapse = ", ")
  ))
}
