# Run from the project root.
source("R/scale_series.R", local = TRUE)
source("R/example_inputs.R", local = TRUE)

result <- tsflow::execute_pipeline(
  "pipeline.csv", inputs = example_inputs(),
  transforms = list(scale_series = scale_series),
  error_mode = "collect"
)
result$values$adjusted
result$errors
result$missing_inputs

# Optional caching: add store = "_tsflow" to execute_pipeline().
# See the generated setup guide for targets and the next development steps.
