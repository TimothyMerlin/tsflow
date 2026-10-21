# Run from the project root with targets::tar_make().
library(targets)
source("R/scale_series.R", local = TRUE)
source("R/example_inputs.R", local = TRUE)

list(
  tar_target(tsflow_inputs, example_inputs()),
  tsflow::tar_tsflow(
    "pipeline.csv",
    transforms = list(scale_series = scale_series)
  )
)
