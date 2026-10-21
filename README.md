# tsflow

`tsflow` defines and runs declarative time-series pipelines.

The current MVP contract is:

- raw series are injected at runtime as named `ts` objects
- derived series are evaluated as ordered pipeline steps
- transforms resolve as ordinary R functions
- YAML is the canonical structured pipeline shape
- CSV plus a sibling sidecar YAML file is a supported authoring convenience

## Start a Project or Package

Create a complete pipeline package, including an example transform, generated
registry, tests, and a setup walkthrough:

```r
tsflow::create_tsflow_package("mypipeline", targets = TRUE)
```

Or start an ordinary analysis project without a package:

```r
tsflow::use_tsflow("myanalysis")
```

Both support direct execution and optional targets execution. See
`vignette("getting-started", package = "tsflow")` for the first run, adding
transforms, and enabling targets later.

## CSV Workflow

The main spreadsheet-style format is:

```text
name,input,step_1,step_2,...
```

Each step column is interpreted left to right as a pipe step. A sibling sidecar
file with the same stem can provide shared values:

- `pipeline.csv`
- `pipeline.meta.yml`

The sidecar may define:

- `vars`
- `outputs`
- `metadata`

CSV step cells can reference sidecar vars with `@name`.

Example CSV:

```csv
name,input,step_1,step_2
gdp.nominal.quarterly,gdp.nominal.quarterly.sa,"jsa(gdp.nominal.annual.nsa, method=@jsa_method)",
construction.deflator.quarterly,construction.nominal.quarterly,div(construction.real.quarterly),mul(100)
```

Example sidecar:

```yaml
vars:
  jsa_method: denton-cholette
  base_year: 2021
  conversion: sum
```

YAML remains the canonical structured shape. CSV is compiled into the same
internal step representation before normalization, validation, DAG planning, and
execution.

## Plan Then Execute

Plan the pipeline first:

```r
# Transform functions as defined in vignette("gdp-construction")
transforms <- list(jsa = jsa, real_gr_nom = real_gr_nom, a2q_sum = a2q_sum)

plan <- plan_pipeline(
  path = "inst/examples/gdp_construction.csv",
  transforms = transforms
)

plan
dag_edges(plan$dag, include_labels = TRUE)
```

Then execute it with injected inputs:

```r
data("gdp_construction_inputs", package = "tsflow")

result <- execute_pipeline(
  path = "inst/examples/gdp_construction.csv",
  inputs = gdp_construction_inputs,
  transforms = transforms
)
```

`result$values` contains the full executed state. By default,
`result$outputs` contains derived series only. Pass `outputs = ...` to execute
and materialize only the upstream subgraph needed for one or more targets.

In interactive sessions, execution shows a progress bar automatically when
`progressr` is installed. Existing user handlers are respected, and progress can
be disabled with `progress = FALSE` or `options(tsflow.progress = FALSE)`.

`execute_pipeline()` runs series one after another. For parallel execution,
use `tar_tsflow()` with a `targets` parallel backend (see "Running With
targets").

Transforms can be referenced as bare names such as `jsa` or namespaced
functions such as `stats::lag`. Bare names must be registered in the
`transforms` list; namespaced functions are resolved directly from the package
namespace. To restrict which packages a pipeline may call, set
`options(tsflow.allowed_packages = c("stats", "tempdisagg"))`.

## Loading Raw Data

Instead of passing every raw series through `inputs`, a series' `source` can
say where its data comes from, and a loader for that source type fetches it:

```yaml
series:
  - name: gdp.nominal.annual
    source: {type: csv, file: data/gdp.csv, column: nominal}
```

```r
loaders <- list(
  csv = function(source, name) read_my_csv(source$file, source$column)
)

result <- execute_pipeline("gdp.yml", loaders = loaders, transforms = transforms)
```

A loader is called with the series' `source` entry (without `type`) and its
name, and returns a `ts` or `xts`. A series in `inputs` is used instead of its
loader. If a raw series has no loader and is not in `inputs`, or its loader
returns `NULL`, it is a missing input: it and everything depending on it are
skipped with a warning, and the rest still runs (`result$missing_inputs`). A
loader that throws an error is a failure like any other step.
`plan_pipeline(loaders = ...)$needs_inputs` lists the raw series without a
loader.

## Caching Results

To keep results between runs and recompute only what changed, give
`execute_pipeline()` a cache directory, or set it once for a whole script:

```r
options(tsflow.store = "_tsflow_cache")

result <- execute_pipeline(
  path = "inst/examples/gdp_construction.csv",
  inputs = gdp_construction_inputs,
  transforms = transforms
)

result$rebuilt   # series recomputed in this run
```

This uses [targets](https://docs.ropensci.org/targets/) behind the scenes; no
`_targets.R` is needed. Changes to inputs, parameters, transforms and the
functions they use are detected. Don't run two R processes on the same store
at the same time.

## Running With targets

For larger or regularly updated pipelines, `tar_tsflow()` turns a pipeline into
a [targets](https://docs.ropensci.org/targets/) pipeline. `targets` then reruns
only the steps whose inputs, parameters or transforms changed, keeps results
between sessions, and can run steps in parallel.

```r
# _targets.R
library(targets)
library(tsflow)

# jsa, real_gr_nom, a2q_sum as defined in vignette("gdp-construction")

list(
  tar_target(tsflow_inputs, load_my_inputs()),   # named list of raw series
  tar_tsflow(
    "gdp_construction.csv",
    transforms = list(jsa = jsa, real_gr_nom = real_gr_nom, a2q_sum = a2q_sum)
  )
)
```

Run it with `targets::tar_make()` and read a series with
`targets::tar_read(construction.deflator.quarterly)`. By default each step is
its own target; use `granularity = "series"` for one target per series.
`vignette("targets")` walks through caching, a full `targets` project, tracked
data files, missing inputs and documentation as a target.

## Execution Semantics

- `result$values` contains the full executed state for the run.
- `result$outputs` contains the materialized user-facing outputs.
- If `outputs = NULL`, `result$outputs` defaults to derived series only.
- If `outputs = ...` is provided, execution is pruned to the upstream subgraph
  needed for those targets.
- Arithmetic steps (`add`, `sub`, `mul`, `div`, or `+ - * /` in `expr`)
  between two series combine them on their common period; observations
  outside the overlap are dropped. Non-overlapping series, `ts` series with
  different frequencies, and mixing `ts` with `xts` are errors.
- Series are evaluated one after another in dependency order. With
  `error_mode = "fail"` the first error stops the run; with `"collect"` it is
  recorded and only the series depending on it are skipped.

## DAG Inspection

Planning produces a dependency DAG. Useful helpers include:

- `dag_edges()`
- `upstream_nodes()`
- `downstream_nodes()`
- `dag_to_igraph()`
- `dag_to_visnetwork()`

For interactive inspection in HTML output, use `dag_to_visnetwork()`. The
packaged vignette shows that workflow with a fixed left-to-right layout. Clicking
a node highlights the upstream calculation path used to produce it and fades
unrelated nodes. The widget also shows the selected node and its upstream path
in an on-page details panel.

## Lineage Documentation

`document_pipeline()` writes one Markdown page per series plus an index: how
the series is defined step by step, which input feeds which transform argument,
the shared `vars` it uses, a Mermaid diagram of its upstream lineage down to
the raw sources, and the series that use it. The pages render on GitHub, in
pkgdown and in Quarto.

```r
plan <- plan_pipeline("gdp_construction.csv", transforms = transforms)

document_pipeline(
  plan,
  "docs/lineage",
  transform_descriptions = c(a2q_sum = "Annual to quarterly, Chow-Lin"),
  result = result   # optional: adds status, period and latest value
)
```

`series_lineage(plan, "construction.real.quarterly")` returns the same
information for one series as an R list, for building other formats.

## Example Files

The package includes:

- `inst/examples/gdp_construction.csv`
- `inst/examples/gdp_construction.meta.yml`
- `inst/examples/gdp_construction_transforms.R`
- `vignettes/gdp-construction.Rmd`

The vignette is the best end-to-end walkthrough.
