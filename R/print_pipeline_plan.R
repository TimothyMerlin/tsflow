#' Print a Pipeline Plan
#'
#' @param x A `PipelinePlan` object.
#' @param ... Unused.
#'
#' @method print PipelinePlan
#' @export
print.PipelinePlan <- function(x, ...) {
  cat("<PipelinePlan>\n")
  cat(sprintf("  nodes: %d\n", length(x$dag$nodes)))
  cat(sprintf("  outputs: %d\n", length(x$outputs)))
  cat(sprintf("  execution order: %d\n", length(x$dag$order)))

  if (length(x$outputs) > 0L) {
    cat(sprintf("  output names: %s\n", paste(x$outputs, collapse = ", ")))
  }

  if (length(x$needs_inputs) > 0L) {
    cat(sprintf("  needs `inputs` (no loader): %s\n", paste(x$needs_inputs, collapse = ", ")))
  }

  invisible(x)
}
