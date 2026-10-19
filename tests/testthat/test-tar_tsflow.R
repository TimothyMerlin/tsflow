write_tar_pipeline <- function() {
  writeLines(
    c(
      "vars:",
      "  factor: 10",
      "series:",
      "  - name: raw_a",
      "    source: {type: injected}",
      "  - name: prices",
      "    source: {type: injected}",
      "  - name: scaled",
      "    expr: \"scale(raw_a, factor = @factor)\"",
      "  - name: real",
      "    expr: \"growth(scaled) / prices\""
    ),
    "pipeline.yml"
  )
}

write_targets_script <- function(lines) {
  writeLines(
    c(
      "library(targets)",
      "scale <- function(x, factor) x * factor",
      "growth <- function(x) x + 1",
      lines
    ),
    "_targets.R"
  )
}

tar_make_quiet <- function() {
  targets::tar_make(reporter = "silent", callr_function = NULL)
}

targets_inputs_line <- paste0(
  "  tar_target(tsflow_inputs, list(raw_a = ts(c(1, 2, 3)), prices = ts(c(2, 2, 4)))),"
)

targets::tar_test("tar_tsflow creates one target per step and matches evaluate_pipeline", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  write_targets_script(c(
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = list(scale = scale, growth = growth))",
    ")"
  ))

  manifest <- targets::tar_manifest(callr_function = NULL)
  expect_setequal(
    manifest$name,
    c("tsflow_inputs", "raw_a", "prices", "scaled", "real..step1", "real")
  )

  tar_make_quiet()

  scale <- function(x, factor) x * factor
  growth <- function(x) x + 1
  expected <- execute_pipeline(
    "pipeline.yml",
    inputs = list(raw_a = ts(c(1, 2, 3)), prices = ts(c(2, 2, 4))),
    transforms = list(scale = scale, growth = growth),
    progress = FALSE
  )
  expect_equal(targets::tar_read(real), expected$values$real)
  expect_equal(targets::tar_read(scaled), expected$values$scaled)
})

targets::tar_test("tar_tsflow reruns only the steps whose transform changed", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  body <- c(
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = list(scale = scale, growth = growth))",
    ")"
  )
  write_targets_script(body)
  tar_make_quiet()
  expect_length(targets::tar_outdated(callr_function = NULL), 0L)

  writeLines(
    c("library(targets)", "scale <- function(x, factor) x * factor", "growth <- function(x) x + 2", body),
    "_targets.R"
  )
  # growth is step 1 of `real`; its downstream op step depends on it too.
  expect_setequal(targets::tar_outdated(callr_function = NULL), c("real..step1", "real"))
})

targets::tar_test("tar_tsflow series granularity creates one target per series", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  write_targets_script(c(
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = list(scale = scale, growth = growth), granularity = 'series')",
    ")"
  ))

  manifest <- targets::tar_manifest(callr_function = NULL)
  expect_setequal(manifest$name, c("tsflow_inputs", "raw_a", "prices", "scaled", "real"))

  tar_make_quiet()
  expect_equal(as.numeric(targets::tar_read(real)), c(5.5, 10.5, 7.75))
})

targets::tar_test("tar_tsflow limits targets to requested outputs", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  write_targets_script(c(
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = list(scale = scale, growth = growth), outputs = 'scaled')",
    ")"
  ))

  manifest <- targets::tar_manifest(callr_function = NULL)
  expect_setequal(manifest$name, c("tsflow_inputs", "raw_a", "scaled"))
})

targets::tar_test("tar_tsflow tracks a registry variable by matching global names, else the registry", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  write_targets_script(c(
    "renamed_growth <- function(x) x + 3",
    "registry <- list(scale = scale, growth = renamed_growth)",
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = registry)",
    ")"
  ))

  expect_message(
    manifest <- targets::tar_manifest(callr_function = NULL),
    "transforms `growth` have no global function of the same name"
  )
  commands <- stats::setNames(manifest$command, manifest$name)
  expect_match(commands[["scaled"]], "transforms = list(scale = scale)", fixed = TRUE)
  expect_match(commands[["real..step1"]], "transforms = list(growth = registry[[\"growth\"]])", fixed = TRUE)
})

targets::tar_test("tar_tsflow records package versions of namespaced transforms", {
  skip_if_not_installed("targets")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source: {type: injected}",
      "  - name: lagged",
      "    expr: \"stats::lag(raw_a, -1)\""
    ),
    "pipeline.yml"
  )
  write_targets_script(c(
    "list(",
    "  tar_target(tsflow_inputs, list(raw_a = ts(1:3))),",
    "  tar_tsflow('pipeline.yml')",
    ")"
  ))

  manifest <- targets::tar_manifest(callr_function = NULL)
  command <- manifest$command[manifest$name == "lagged"]
  expect_match(command, sprintf("stats = \"%s\"", utils::packageVersion("stats")), fixed = TRUE)

  tar_make_quiet()
  expect_equal(targets::tar_read(lagged), stats::lag(ts(1:3), -1))
})

targets::tar_test("tar_tsflow applies hooks", {
  skip_if_not_installed("targets")
  write_tar_pipeline()
  write_targets_script(c(
    "subtract <- function(op, left, right) left - right",
    "list(",
    targets_inputs_line,
    "  tar_tsflow('pipeline.yml', transforms = list(scale = scale, growth = growth),",
    "             hooks = list(binary_op = subtract))",
    ")"
  ))

  tar_make_quiet()
  expect_equal(as.numeric(targets::tar_read(real)), c(9, 19, 27))
})

test_that("tar_tsflow validates its arguments", {
  skip_if_not_installed("targets")
  path <- tempfile(fileext = ".yml")
  writeLines(c("series:", "  - name: raw_a", "    source: {type: injected}"), path)

  expect_error(tar_tsflow(path, inputs = c("a", "b")), "`inputs` must be the name of a target")
  expect_error(tar_tsflow(path, inputs = "raw_a"), "same name as a series")
  expect_error(tar_tsflow(path, outputs = "missing"), "`outputs` are not series in the pipeline: missing")
  expect_error(tar_tsflow(path, granularity = "node"), "should be one of")
})
