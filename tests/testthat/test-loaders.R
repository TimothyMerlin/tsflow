loader_pipeline_lines <- function() {
  c(
    "series:",
    "  - name: from_csv",
    "    source: {type: csv, file: a.csv}",
    "  - name: from_db",
    "    source: {type: db, key: k1}",
    "  - name: injected_c",
    "    source: {type: injected}",
    "  - name: a_scaled",
    "    expr: \"from_csv * 10\"",
    "  - name: db_scaled",
    "    expr: \"from_db * 2\"",
    "  - name: combined",
    "    expr: \"a_scaled + db_scaled\""
  )
}

write_loader_pipeline <- function(path) {
  writeLines(loader_pipeline_lines(), path)
  path
}

test_that("loaders provide raw series from their source config", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  seen <- list()
  loaders <- list(
    csv = function(source, name) {
      seen[[name]] <<- source
      ts(c(1, 2, 3))
    },
    db = function(source, name) ts(c(5, 5, 5))
  )

  result <- execute_pipeline(path, inputs = list(injected_c = ts(1:3)), loaders = loaders, progress = FALSE)

  expect_equal(seen$from_csv, list(file = "a.csv"))
  expect_equal(as.numeric(result$values$combined), c(20, 30, 40))
  expect_length(result$missing_inputs, 0L)
})

test_that("inputs take precedence over loaders", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  loaders <- list(csv = function(source, name) stop("should not be called"), db = function(source, name) ts(c(1, 1, 1)))

  result <- execute_pipeline(
    path,
    inputs = list(from_csv = ts(c(9, 9, 9)), injected_c = ts(1:3)),
    loaders = loaders,
    progress = FALSE
  )
  expect_equal(as.numeric(result$values$a_scaled), c(90, 90, 90))
})

test_that("missing inputs skip their dependents but never stop the run", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  # db returns NULL ("not published yet"); injected_c is not given at all.
  loaders <- list(csv = function(source, name) ts(c(1, 2, 3)), db = function(source, name) NULL)

  for (mode in c("fail", "collect")) {
    expect_warning(
      result <- execute_pipeline(path, loaders = loaders, error_mode = mode, progress = FALSE),
      "missing: from_db, injected_c"
    )
    expect_equal(result$missing_inputs, c("from_db", "injected_c"))
    expect_equal(result$skipped, c("db_scaled", "combined"))
    expect_equal(as.numeric(result$values$a_scaled), c(10, 20, 30))
    expect_null(result$values$combined)
    expect_equal(result$dag$node_status[["from_db"]], "missing_input")
    expect_equal(result$dag$node_status[["combined"]], "skipped")
    expect_length(result$errors, 0L)
  }
})

test_that("loader errors are failures, not missing inputs", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  loaders <- list(
    csv = function(source, name) ts(c(1, 2, 3)),
    db = function(source, name) stop("database unreachable")
  )
  inputs <- list(injected_c = ts(1:3))

  expect_error(
    execute_pipeline(path, inputs = inputs, loaders = loaders, progress = FALSE),
    "failed while evaluating series `from_db`: loader `db` failed: database unreachable",
    fixed = TRUE
  )

  expect_warning(
    result <- execute_pipeline(path, inputs = inputs, loaders = loaders, error_mode = "collect", progress = FALSE),
    "failed: from_db"
  )
  expect_match(result$errors$from_db, "loader `db` failed: database unreachable", fixed = TRUE)
  expect_equal(result$skipped, c("db_scaled", "combined"))
  expect_length(result$missing_inputs, 0L)
  expect_equal(as.numeric(result$values$a_scaled), c(10, 20, 30))
})

test_that("loaders must return ts or xts", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  loaders <- list(csv = function(source, name) 1:3, db = function(source, name) ts(1:3))

  expect_error(
    execute_pipeline(path, inputs = list(injected_c = ts(1:3)), loaders = loaders, progress = FALSE),
    "raw input `from_csv` must be an R `ts` or `xts` object",
    fixed = TRUE
  )
})

test_that("loaders are validated", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  expect_error(execute_pipeline(path, loaders = list(function(s, n) NULL)), "named list of functions")
  expect_error(execute_pipeline(path, loaders = list(csv = 1)), "entries must be functions: csv")
  expect_error(execute_pipeline(path, loaders = list(injected = identity)), "cannot define `injected`")
})

test_that("plan_pipeline reports raw series that need inputs", {
  path <- write_loader_pipeline(tempfile(fileext = ".yml"))
  plan <- plan_pipeline(path, loaders = list(csv = function(source, name) NULL))

  expect_equal(plan$needs_inputs, c("from_db", "injected_c"))
  expect_output(print(plan), "needs `inputs` (no loader): from_db, injected_c", fixed = TRUE)
})

targets::tar_test("tar_tsflow loads raw series with loaders and reruns them every time", {
  skip_if_not_installed("targets")
  writeLines(loader_pipeline_lines(), "pipeline.yml")
  writeLines(
    c(
      "library(targets)",
      "read_csv_series <- function(source, name) ts(c(1, 2, 3))",
      "read_db_series <- function(source, name) NULL",
      "list(",
      "  tar_target(tsflow_inputs, list(injected_c = ts(1:3))),",
      "  tsflow::tar_tsflow('pipeline.yml', loaders = list(csv = read_csv_series, db = read_db_series))",
      ")"
    ),
    "_targets.R"
  )

  targets::tar_make(reporter = "silent", callr_function = NULL)
  expect_equal(as.numeric(targets::tar_read(a_scaled)), c(10, 20, 30))
  expect_s3_class(targets::tar_read(from_db), "tsflow_missing_input")
  expect_equal(targets::tar_read(combined)$names, "from_db")

  # Loader targets always rerun; unchanged data doesn't rerun downstream steps.
  expect_true(all(c("from_csv", "from_db") %in% targets::tar_outdated(callr_function = NULL)))
  targets::tar_make(reporter = "silent", callr_function = NULL)
  progress <- targets::tar_progress()
  expect_equal(progress$progress[progress$name == "from_csv"], "completed")
  expect_equal(progress$progress[progress$name == "a_scaled"], "skipped")
})

test_that("cached execution supports loaders and missing inputs", {
  skip_if_not_installed("targets")
  skip_if_not_installed("codetools")
  dir <- withr::local_tempdir()
  path <- write_loader_pipeline(file.path(dir, "p.yml"))
  store <- file.path(dir, "cache")
  db_value <- NULL
  loaders <- list(csv = function(source, name) ts(c(1, 2, 3)), db = function(source, name) db_value)

  expect_warning(
    first <- execute_pipeline(path, loaders = loaders, progress = FALSE, store = store),
    "missing: from_db, injected_c"
  )
  expect_equal(first$missing_inputs, c("from_db", "injected_c"))
  expect_equal(first$skipped, c("db_scaled", "combined"))
  expect_equal(as.numeric(first$values$a_scaled), c(10, 20, 30))

  # The database now has data: only its dependents are recomputed.
  db_value <- ts(c(5, 5, 5))
  second <- execute_pipeline(path, inputs = list(injected_c = ts(1:3)), loaders = loaders, progress = FALSE, store = store)
  expect_length(second$missing_inputs, 0L)
  expect_setequal(second$rebuilt, c("db_scaled", "combined"))
  expect_equal(as.numeric(second$values$combined), c(20, 30, 40))
})
