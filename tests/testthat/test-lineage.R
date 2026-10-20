lineage_plan <- function() {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "vars:",
      "  factor: 10",
      "series:",
      "  - name: raw_a",
      "    description: Raw input A",
      "    source: {type: csv, file: a.csv}",
      "  - name: raw_b",
      "    source: {type: injected}",
      "  - name: scaled",
      "    description: A scaled",
      "    metadata: {unit: CHF, owner: kof}",
      "    expr: \"scale(raw_a, factor = @factor)\"",
      "  - name: combined",
      "    expr: \"scaled / raw_b\""
    ),
    path
  )
  scale <- function(x, factor) x * factor
  plan_pipeline(path, transforms = list(scale = scale))
}

test_that("series_lineage describes a derived series", {
  plan <- lineage_plan()
  lineage <- series_lineage(plan, "scaled", transform_descriptions = c(scale = "Multiply by a factor"))

  expect_s3_class(lineage, "SeriesLineage")
  expect_equal(lineage$kind, "derived")
  expect_equal(lineage$description, "A scaled")
  expect_equal(lineage$input, "raw_a")
  expect_equal(lineage$steps$text, "scale(factor=10)")
  expect_equal(lineage$steps$vars, "factor")
  expect_equal(lineage$steps$description, "Multiply by a factor")
  expect_equal(lineage$inputs$series, "raw_a")
  expect_equal(lineage$inputs$used_as, "scale: x")
  expect_equal(lineage$upstream, "raw_a")
  expect_equal(lineage$raw_sources$type, "csv")
  expect_equal(lineage$raw_sources$config, "file = a.csv")
  expect_equal(lineage$downstream, "combined")
  expect_null(lineage$status)
})

test_that("series_lineage follows lineage through several levels", {
  lineage <- series_lineage(lineage_plan(), "combined")

  expect_setequal(lineage$upstream, c("raw_a", "scaled", "raw_b"))
  expect_setequal(lineage$raw_sources$series, c("raw_a", "raw_b"))
  expect_equal(lineage$steps$text, "div(raw_b)")
  expect_equal(nrow(lineage$graph_edges), 3L)
  expect_length(lineage$downstream, 0L)
})

test_that("series_lineage describes raw series", {
  lineage <- series_lineage(lineage_plan(), "raw_a")

  expect_equal(lineage$kind, "raw")
  expect_equal(lineage$source$type, "csv")
  expect_equal(nrow(lineage$steps), 0L)
  expect_setequal(lineage$downstream, c("scaled", "combined"))
})

test_that("series_lineage adds status, period and latest value from a result", {
  plan <- lineage_plan()
  scale <- function(x, factor) x * factor
  result <- evaluate_pipeline(
    plan,
    inputs = list(raw_a = ts(c(1, 2, 3, 4), start = c(2020, 1), frequency = 4)),
    transforms = list(scale = scale),
    progress = FALSE
  ) |> suppressWarnings()

  scaled <- series_lineage(plan, "scaled", result = result)
  expect_equal(scaled$status$status, "ok")
  expect_equal(scaled$status$period, "2020Q1 to 2020Q4")
  expect_equal(scaled$status$latest_value, "40")

  expect_equal(series_lineage(plan, "raw_b", result = result)$status$status, "missing_input")
  expect_equal(series_lineage(plan, "combined", result = result)$status$status, "skipped")
  expect_true(is.na(series_lineage(plan, "combined", result = result)$status$period))

  # A result can be passed as the plan itself.
  expect_equal(series_lineage(result, "scaled")$status$status, "ok")
})

test_that("document_pipeline writes an index and one Markdown page per series", {
  dir <- withr::local_tempdir()
  files <- document_pipeline(lineage_plan(), dir, transform_descriptions = c(scale = "Multiply by a factor"))

  expect_setequal(basename(files), c("index.md", "raw_a.md", "raw_b.md", "scaled.md", "combined.md"))

  page <- readLines(file.path(dir, "scaled.md"))
  expect_equal(page[[1L]], "# scaled")
  expect_true("*Derived series* · A scaled" %in% page)
  expect_true("- **unit:** CHF" %in% page)
  expect_true("1. Start from [raw_a](raw_a.md)" %in% page)
  expect_true("2. `scale(factor=10)` — uses var `factor`; Multiply by a factor" %in% page)
  expect_true("| [raw_a](raw_a.md) | scale: x |" %in% page)
  expect_true("```mermaid" %in% page)
  expect_true(any(grepl("^  n[0-9]+ --> n[0-9]+$", page)))
  expect_true("- [combined](combined.md)" %in% page)

  raw_page <- readLines(file.path(dir, "raw_a.md"))
  expect_true("Type `csv`: file = a.csv" %in% raw_page)

  index <- readLines(file.path(dir, "index.md"))
  expect_true("| [scaled](scaled.md) | derived | A scaled |" %in% index)
})

test_that("lineage functions validate their arguments", {
  plan <- lineage_plan()
  expect_error(series_lineage(plan, "missing"), "node `missing` is not present")
  expect_error(series_lineage(list(), "scaled"), "must be a `PipelinePlan`")
  expect_error(series_lineage(plan, "scaled", transform_descriptions = "x"), "named character vector")
  expect_error(document_pipeline(plan, c("a", "b")), "`dir` must be a single directory path")
})

test_that("var names are kept on normalized literals", {
  spec <- normalize_pipeline(list(
    vars = list(f = 2),
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(name = "s", expr = "scale(raw_a, factor = @f, other = 3)")
    )
  ))
  kwargs <- spec$series[[2]]$expression$call$kwargs
  expect_equal(kwargs$factor$value, 2)
  expect_equal(kwargs$factor$var, "f")
  expect_null(kwargs$other$var)
})
