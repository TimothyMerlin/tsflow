# Run from the package root with targets::tar_make().
library(targets)
pkgload::load_all(".")
registry <- tsflow::package_registry("{{package}}")

list(
  tar_target(tsflow_inputs, example_inputs()),
  tsflow::tar_tsflow(
    "inst/extdata/pipeline.csv",
    transforms = registry$transforms,
    hooks = registry$hooks
  )
)
