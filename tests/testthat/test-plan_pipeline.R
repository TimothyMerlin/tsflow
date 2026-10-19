test_that("plan_pipeline returns a non-executing pipeline plan", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10",
      "outputs:",
      "  - derived_b"
    ),
    con = path
  )

  plan <- plan_pipeline(
    path = path,
    transforms = local_transforms()
  )

  expect_s3_class(plan, "PipelinePlan")
  expect_s3_class(plan$pipeline, "PipelineSpec")
  expect_s3_class(plan$dag, "DAGPlan")
  expect_equal(plan$outputs, "derived_b")
  expect_equal(plan$dag$order, c("raw_a", "derived_b"))
})

test_that("plan_pipeline lets explicit outputs override YAML outputs", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10"
    ),
    con = path
  )

  plan <- plan_pipeline(
    path = path,
    outputs = "raw_a",
    transforms = local_transforms()
  )

  expect_equal(plan$outputs, "raw_a")
})

test_that("plan_pipeline validates requested outputs and transform resolution", {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: missing_scale",
      "          args:",
      "            - 10"
    ),
    con = path
  )

  expect_error(
    plan_pipeline(
      path = path,
      transforms = list()
    ),
    "missing_scale"
  )

  valid_path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10"
    ),
    con = valid_path
  )
  scale <- function(x, factor) x * factor

  expect_error(
    plan_pipeline(
      path = valid_path,
      outputs = "missing_output",
      transforms = local_transforms()
    ),
    "planned outputs are not present in the pipeline"
  )
})

test_that("print.PipelinePlan shows a concise summary", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: injected",
      "  - name: derived_b",
      "    steps:",
      "      - input: raw_a",
      "      - call:",
      "          name: scale",
      "          args:",
      "            - 10",
      "outputs:",
      "  - derived_b"
    ),
    con = path
  )

  plan <- plan_pipeline(
    path = path,
    transforms = local_transforms()
  )

  output <- paste(capture.output(print(plan)), collapse = "\n")

  expect_match(output, "<PipelinePlan>")
  expect_match(output, "nodes: 2")
  expect_match(output, "outputs: 1")
  expect_match(output, "execution order: 2")
  expect_match(output, "output names: derived_b")
})

test_that("plan_pipeline plans wide CSV mappings through the normal DAG path", {
  scale <- function(x, factor) x * factor

  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_b,raw_a,scale(10)"
    ),
    con = path
  )

  plan <- plan_pipeline(
    path = path,
    transforms = local_transforms()
  )

  expect_s3_class(plan, "PipelinePlan")
  expect_equal(plan$dag$order, c("raw_a", "derived_b"))
  expect_equal(plan$outputs, "derived_b")
})

test_that("plan_pipeline can retain cycle nodes for debugging", {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: series_a",
      "    steps:",
      "      - input: series_b",
      "  - name: series_b",
      "    steps:",
      "      - input: series_a"
    ),
    con = path
  )

  plan <- plan_pipeline(
    path = path,
    transforms = local_transforms(),
    allow_cycles = TRUE
  )

  expect_s3_class(plan, "PipelinePlan")
  expect_false(plan$dag$is_acyclic)
  expect_equal(plan$dag$cycle_nodes, c("series_a", "series_b"))
  expect_equal(plan$outputs, c("series_a", "series_b"))
})

test_that("plan_pipeline formats vector vars in node pipeline summaries", {
  a2q_sum <- function(x, end) x

  dir <- tempfile()
  dir.create(dir)
  path <- file.path(dir, "mapping.csv")
  meta_path <- file.path(dir, "mapping.meta.yml")

  writeLines(
    c(
      "name,input,step_1",
      "derived_b,raw_a,\"a2q_sum(end=@cutoff)\""
    ),
    con = path
  )
  writeLines(
    c(
      "vars:",
      "  cutoff:",
      "    - 2026",
      "    - 4"
    ),
    con = meta_path
  )

  plan <- plan_pipeline(
    path = path,
    transforms = local_transforms()
  )

  expect_equal(plan$dag$node_pipeline$derived_b$steps, "a2q_sum(end=c(2026, 4))")
})
