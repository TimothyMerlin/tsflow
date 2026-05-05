# tsflow

`tsflow` defines and runs declarative time-series pipelines.

The current MVP contract is:

- raw series are injected at runtime as named `ts` objects
- derived series are evaluated as ordered pipeline steps
- transforms resolve as ordinary R functions
- YAML is the canonical structured pipeline shape
- CSV plus a sibling sidecar YAML file is a supported authoring convenience

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
plan <- plan_pipeline(
  path = "inst/examples/gdp_construction.csv",
  transform_env = environment()
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
  transform_env = environment()
)
```

`result$values` contains the full executed state. By default,
`result$outputs` contains derived series only. Pass `outputs = ...` to execute
and materialize only the upstream subgraph needed for one or more targets.

In interactive sessions, execution shows a progress bar automatically when
`progressr` is installed. Existing user handlers are respected, and progress can
be disabled with `progress = FALSE` or `options(tsflow.progress = FALSE)`.

If `future` and `future.apply` are installed, independent DAG frontiers can be
executed using the current `future::plan()`. The plan controls the actual
parallel backend and worker count:

```r
future::plan(future::multisession, workers = 2)

result <- execute_pipeline(
  path = "inst/examples/gdp_construction.csv",
  inputs = gdp_construction_inputs,
  transform_env = environment(),
  outputs = "construction.nominal.quarterly"
)
```

Transforms can be referenced as bare names such as `jsa` or namespaced
functions such as `stats::lag`. Bare names are resolved from `transform_env`;
namespaced functions are resolved directly from the package namespace.

## Execution Semantics

- `result$values` contains the full executed state for the run.
- `result$outputs` contains the materialized user-facing outputs.
- If `outputs = NULL`, `result$outputs` defaults to derived series only.
- If `outputs = ...` is provided, execution is pruned to the upstream subgraph
  needed for those targets.
- If `future` and `future.apply` are available, parallel frontier execution is
  enabled by the current `future::plan()`.
- The actual parallel backend and worker count are controlled entirely by that
  `future::plan()`.

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

## Example Files

The package includes:

- `inst/examples/gdp_construction.csv`
- `inst/examples/gdp_construction.meta.yml`
- `vignettes/gdp-construction.Rmd`

The vignette is the best end-to-end walkthrough.
