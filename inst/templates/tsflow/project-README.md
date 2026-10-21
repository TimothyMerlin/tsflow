# tsflow pipeline

<!-- tsflow setup guide -->

Run commands below from the project root.

## Run the example

```r
source("run.R")
```

`result$values$adjusted` contains quarterly values 20, 40, 60, and 80.
`result$errors` and `result$missing_inputs` report problems without stopping
independent series from running.

## Make it yours

1. Replace the sample values in `R/example_inputs.R` with your own inputs.
2. Edit `pipeline.csv` to define series and transform steps. Set shared variables
   and requested outputs in `pipeline.meta.yml`.
3. Add functions under `R/`, source them in `run.R`, and register them explicitly
   in its `transforms` list. The first function argument receives the incoming
   series; the CSV supplies any other arguments.

No package structure is required. To grow into a package, start with
`tsflow::create_tsflow_package("mypipeline")` and move your pipeline and functions
into that structure. See `vignette("getting-started", package = "tsflow")`.

## Optional targets execution

Install `targets` first if needed with `install.packages("targets")`.

```r
tsflow::use_tsflow(targets = TRUE)
targets::tar_make()
targets::tar_read(adjusted)
```

The starter `_targets.R` uses the same pipeline and input function. If you have
added transforms, also source and register them there. Existing files are never
overwritten. Keep `_targets/` and any `_tsflow/` cache out of version control.
