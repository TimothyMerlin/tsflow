# Time alignment for tsflow's own `add`/`sub`/`mul`/`div` steps.
#
# Partly overlapping series are combined on their common period (R's own
# behaviour for `ts` and `xts`). The cases where R fails cryptically or
# silently gives a wrong or empty result are turned into clear errors.

check_aligned_operands <- function(op, left, right, labels) {
  left_ts <- stats::is.ts(left)
  right_ts <- stats::is.ts(right)
  left_xts <- inherits(left, "xts")
  right_xts <- inherits(right, "xts")

  if (!(left_ts || left_xts) || !(right_ts || right_xts)) {
    return(invisible(NULL))
  }

  verb <- c(add = "add", sub = "subtract", mul = "multiply", div = "divide")[[op]]
  what <- sprintf("cannot %s `%s` and `%s`", verb, labels[[1L]], labels[[2L]])

  # Mixing classes makes R drop the dates and combine values by position.
  if (left_ts != right_ts) {
    fail(sprintf(
      "%s: one is a `ts` and the other an `xts`; convert the inputs to one class",
      what
    ))
  }

  if (left_ts) {
    if (!isTRUE(all.equal(stats::frequency(left), stats::frequency(right)))) {
      fail(sprintf(
        "%s: they have different frequencies (%s and %s)",
        what,
        format(stats::frequency(left)),
        format(stats::frequency(right))
      ))
    }

    tolerance <- 0.5 / stats::frequency(left)
    left_range <- stats::tsp(left)[1:2]
    right_range <- stats::tsp(right)[1:2]
    if (max(left_range[[1L]], right_range[[1L]]) > min(left_range[[2L]], right_range[[2L]]) + tolerance) {
      fail(sprintf(
        "%s: their periods don't overlap (%s and %s)",
        what,
        format_ts_period(left),
        format_ts_period(right)
      ))
    }
    return(invisible(NULL))
  }

  if (length(intersect(as.numeric(zoo::index(left)), as.numeric(zoo::index(right)))) == 0L) {
    fail(sprintf(
      "%s: their periods don't overlap (%s and %s)",
      what,
      format_xts_period(left),
      format_xts_period(right)
    ))
  }

  invisible(NULL)
}

operand_label <- function(expr, fallback) {
  if (inherits(expr, "RefNode")) expr$name else fallback
}

format_ts_period <- function(x) {
  sprintf("%s to %s", format_ts_time(stats::start(x), stats::frequency(x)), format_ts_time(stats::end(x), stats::frequency(x)))
}

format_ts_time <- function(time, frequency) {
  if (frequency == 1) {
    return(as.character(time[[1L]]))
  }
  if (frequency == 4) {
    return(sprintf("%dQ%d", time[[1L]], time[[2L]]))
  }
  if (frequency == 12) {
    return(sprintf("%d-%02d", time[[1L]], time[[2L]]))
  }
  sprintf("%d period %d", time[[1L]], time[[2L]])
}

format_xts_period <- function(x) {
  index <- zoo::index(x)
  sprintf("%s to %s", format(min(index)), format(max(index)))
}
