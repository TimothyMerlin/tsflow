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

write_plan_pipeline <- function() {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source: {type: injected}",
      "  - name: scaled",
      "    expr: \"scale(raw_a, factor = 10)\"",
      "  - name: other",
      "    expr: \"raw_a + 1\""
    ),
    con = path
  )
  path
}

test_that("evaluate_pipeline runs a plan with its planned outputs", {
  scale <- function(x, factor) x * factor
  path <- write_plan_pipeline()
  inputs <- list(raw_a = ts(1:3))

  plan <- plan_pipeline(path, outputs = "scaled", transforms = local_transforms())
  result <- evaluate_pipeline(plan, inputs = inputs, transforms = local_transforms(), progress = FALSE)

  expect_equal(names(result$outputs), "scaled")
  expect_equal(result$plan$order, c("raw_a", "scaled"))
  expect_equal(as.numeric(result$values$scaled), c(10, 20, 30))
  expect_identical(result$pipeline, plan$pipeline)

  all_outputs <- evaluate_pipeline(plan, inputs = inputs, outputs = NULL, transforms = local_transforms(), progress = FALSE)
  expect_setequal(names(all_outputs$outputs), c("scaled", "other"))
})

test_that("evaluate_pipeline checks the plan against the transforms given now", {
  scale <- function(x, factor) x * factor
  plan <- plan_pipeline(write_plan_pipeline(), transforms = local_transforms())

  expect_error(
    evaluate_pipeline(plan, inputs = list(raw_a = ts(1:3)), progress = FALSE),
    "uses unresolved transform `scale`"
  )
})

test_that("evaluate_pipeline refuses plans with cycles", {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: a",
      "    expr: \"b + 1\"",
      "  - name: b",
      "    expr: \"a + 1\""
    ),
    con = path
  )
  plan <- plan_pipeline(path, allow_cycles = TRUE)

  expect_error(
    evaluate_pipeline(plan, progress = FALSE),
    "cannot evaluate a plan whose dependency graph contains a cycle involving: a, b"
  )
})
