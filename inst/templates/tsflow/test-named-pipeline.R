test_that("the {{pipeline}} pipeline validates against the package registry", {
  registry <- tsflow::package_registry("{{package}}")
  path <- system.file("extdata", "{{file}}", package = "{{package}}", mustWork = TRUE)
  plan <- tsflow::plan_pipeline(path, transforms = registry$transforms)
  expect_s3_class(plan, "PipelinePlan")
  # Add expected values and metadata checks for your pipeline here.
})
