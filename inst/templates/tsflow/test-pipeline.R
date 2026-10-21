test_that("the shipped pipeline validates against the package registry", {
  registry <- tsflow::package_registry("{{package}}")
  path <- system.file("extdata", "pipeline.csv", package = "{{package}}")
  plan <- tsflow::plan_pipeline(path, transforms = registry$transforms)
  expect_s3_class(plan, "PipelinePlan")
  expect_equal(plan$needs_inputs, "raw")
})

test_that("the example pipeline produces the expected quarterly values", {
  result <- run_pipeline(progress = FALSE)
  expect_length(result$errors, 0L)
  expect_length(result$missing_inputs, 0L)
  expect_equal(result$values$adjusted, example_inputs()$raw * 20)
})
