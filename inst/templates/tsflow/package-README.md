# {{package}}

<!-- tsflow setup guide -->

A tsflow pipeline package. Run commands below from this directory.

Before sharing the package, fill in its title, description, and authors in
`DESCRIPTION`, and choose a license (for example with
`usethis::use_mit_license()`).

## Run the example

```r
pkgload::load_all()
result <- {{package}}::run_pipeline()
result$values$adjusted
result$errors
result$missing_inputs
```

The example scales quarterly sample data by `@factor`, then multiplies it by
two. Its final values are 20, 40, 60, and 80.

## Make it yours

1. Replace the sample data in `R/example_inputs.R` with your data-loading code.
   Its list names must match the raw series referenced by the pipeline.
2. Edit `inst/extdata/pipeline.csv` to define series and their steps, and
   `pipeline.meta.yml` beside it to set shared variables and outputs.
3. Add a transform with `tsflow::use_tsflow_transform("my_transform")`.
   Implement it in `R/my_transform.R` and update its test. The first argument
   receives the incoming series; the CSV supplies any other arguments.
4. Run `roxygen2::roxygenise()` after adding, renaming, or removing tagged
   functions. This updates `inst/tsflow/registry.yml`; commit that generated
   file with the code. Do not edit it by hand.
5. Run `testthat::test_local()` (install `testthat` first if needed). The starter
   tests validate the pipeline and check the example output. Update the expected
   output as you change your pipeline.

`R/run_pipeline.R` connects the pipeline to the generated registry. Its exported
`run_pipeline()` function also works after installing the package; it accepts
custom inputs and a different pipeline path.

You can also use `devtools::document()` and `devtools::test()` if you already
use devtools; it is not required for this workflow.

## Add another pipeline

```r
tsflow::use_tsflow_pipeline("forecasting")
# For a separate YAML pipeline:
tsflow::use_tsflow_pipeline("sources", format = "yaml")
```

The helper creates pipeline files under `inst/extdata/` and a validation test.
Each starter expects a raw input named `raw`; edit its steps, parameters, and
test for your calculation. Choose its path explicitly when running it:

```r
{{package}}::run_pipeline(path = "inst/extdata/forecasting.csv")
```

Existing files are protected. Your default runner and targets script keep their
current pipeline selection until you edit them.

## Optional targets execution

Install `targets` first if needed with `install.packages("targets")`.

```r
tsflow::use_tsflow(targets = TRUE)
targets::tar_make()
targets::tar_read(adjusted)
```

This adds `_targets.R` using the same pipeline, transforms, and input function.
It preserves your existing files. `source("run.R")` remains available for direct
execution. Keep `_targets/` and any `_tsflow/` cache out of version control.

While developing, `_targets.R` loads the package with `pkgload::load_all()`.
Internal package helper edits without a version bump may require
`targets::tar_invalidate(everything())` before rerunning: development-package
fingerprinting is not yet implemented.

## Advanced customization

Transforms describe individual steps. Hooks replace evaluation behavior across
steps and are usually unnecessary. If you need them, use
`tsflow::use_tsflow_hook("postprocess")` or
`tsflow::use_tsflow_hook("binary_op")`, then run `roxygen2::roxygenise()` again.

For hook contracts and a worked provenance example, see
`vignette("package-hooks", package = "tsflow")`.
