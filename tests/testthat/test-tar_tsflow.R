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

test_that("registered namespace functions contribute package versions", {
  expect_equal(
    tar_package_versions("stats::lag", list(stats::lag, utils::head, sum)),
    c(stats = as.character(utils::packageVersion("stats")),
      utils = as.character(utils::packageVersion("utils")))
  )
  expect_null(tar_package_versions("local", list(function(x) x, NULL)))
})

targets::tar_test("package versions follow registered functions to their users", {
  path <- tempfile(fileext = ".yml")
  writeLines(c(
    "series:",
    "  - name: raw_a", "    source: {type: feed}",
    "  - name: lagged", "    expr: 'shift(raw_a, -1)'",
    "  - name: divided", "    expr: 'lagged / raw_a'"
  ), path)
  for (granularity in c("step", "series")) {
    pipeline <- tar_tsflow(
      path, transforms = list(shift = stats::lag, unused = utils::head),
      hooks = list(postprocess = stats::na.omit, binary_op = utils::head),
      loaders = list(feed = stats::ts), granularity = granularity
    )
    commands <- stats::setNames(lapply(pipeline, function(target) {
      as.list(target$command$expr[[1L]])$packages
    }), vapply(pipeline, function(target) target$name, character(1)))
    stats_version <- c(stats = as.character(utils::packageVersion("stats")))
    both_versions <- c(stats_version, utils = as.character(utils::packageVersion("utils")))
    expect_equal(commands$raw_a, stats_version)
    expect_equal(commands$lagged, if (granularity == "step") stats_version else both_versions)
    expect_equal(commands$divided, if (granularity == "step") both_versions["utils"] else
      both_versions)
  }
})

test_that("load_all packages record a hash of their code with the version", {
  skip_if_not_installed("pkgload")
  write_code <- local_dev_package()
  write_code()
  versions <- function() {
    tar_package_versions(character(), list(getExportedValue("tsflowdevfixture", "transform")))
  }
  first <- versions()
  expect_match(first[["tsflowdevfixture"]], "^0\\.0\\.1\\+[0-9a-f]+$")
  expect_identical(tar_package_versions("tsflowdevfixture::transform"), first)

  write_code()
  expect_identical(versions(), first)
  write_code(multiplier = 3)
  expect_false(identical(versions(), first))
})

test_that("code hashes ignore byte-compilation of closures inside objects", {
  holders <- function(fn) {
    list(
      structure(list(fn = fn), class = "holder"),
      structure(1, validity = fn)
    )
  }
  # Compare hashes, as identical() ignores bytecode.
  hash <- function(fn) secretbase::sha3(tar_code_value(holders(fn), new.env()))
  fn <- function(x) x + 1
  expect_identical(hash(fn), hash(compiler::cmpfun(fn)))
  expect_false(identical(hash(fn), hash(function(x) x + 2)))
})

targets::tar_test("edits to load_all packages rerun the steps that use them", {
  skip_if_not_installed("pkgload")
  write_code <- local_dev_package()
  write_code()
  writeLines(c(
    "series:",
    "  - name: raw_a", "    source: {type: injected}",
    "  - name: namespaced", "    expr: 'tsflowdevfixture::transform(raw_a)'",
    "  - name: registered", "    expr: 'fixture(raw_a)'",
    "  - name: other", "    expr: 'raw_a * 2'"
  ), "pipeline.yml")

  for (granularity in c("step", "series")) {
    unlink("_targets", recursive = TRUE)
    write_code()
    write_targets_script(c(
      "list(tar_target(tsflow_inputs, list(raw_a = ts(10))),",
      "  tsflow::tar_tsflow('pipeline.yml', transforms = list(fixture = tsflowdevfixture::transform),",
      sprintf("    granularity = '%s'))", granularity)
    ))
    tar_make_quiet()
    expect_equal(as.numeric(targets::tar_read(namespaced)), 20)
    expect_equal(as.numeric(targets::tar_read(registered)), 20)

    write_code()
    expect_identical(targets::tar_outdated(callr_function = NULL), character())

    write_code(multiplier = 3)
    expect_setequal(targets::tar_outdated(callr_function = NULL), c("namespaced", "registered"))
    tar_make_quiet()
    expect_equal(as.numeric(targets::tar_read(namespaced)), 30)
    expect_equal(as.numeric(targets::tar_read(registered)), 30)
  }
})
