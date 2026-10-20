# tsflow (development version)

* New `series_names()` returns the series names declared by a pipeline in
  definition order, with an optional `kind` filter (`"raw"` / `"derived"`). It
  accepts a `PipelineSpec`, `PipelinePlan`, or `PipelineResult`.

* New `document_pipeline()` writes lineage / methodology documentation as
  Markdown, one page per series plus an index: the definition step by step,
  which input feeds which transform argument, the shared `vars` used, a
  Mermaid diagram of the upstream lineage, raw sources and downstream users.
  Optional `transform_descriptions` annotate steps, and an optional `result`
  adds status, period and latest value. `series_lineage()` returns the same
  information for one series as an R list (#15).

* New option `tsflow.allowed_packages` restricts which packages namespaced
  `pkg::fn` transforms may resolve from (#5).

* New `tar_tsflow()` turns a pipeline into a `targets` pipeline, with one
  target per step (or per series with `granularity = "series"`), so only steps
  whose inputs, parameters or transforms changed are rerun. Transforms are
  referenced so `targets` tracks changes per transform where possible, and
  updating a package used via `pkg::fn` reruns the steps that use it (#14).

* `execute_pipeline()` gains `store` (default `getOption("tsflow.store")`) to
  cache results between runs with `targets`, without writing a `_targets.R`.
  `options(tsflow.store = "_tsflow_cache")` turns caching on for a whole
  script; only steps whose inputs, parameters, transforms or the workspace
  functions they use changed are recomputed, and `result$rebuilt` lists the
  recomputed series. Each pipeline file gets its own subfolder of the store
  (#24).

* CSV step cells are now parsed with R's own parser (`str2lang()`) instead of
  a hand-written one. Existing cells read exactly as before, and cells can now
  use nested calls (`a2q_sum(jsa(raw_b), conversion = @c)`), arithmetic on
  numbers (`100 / 4`, evaluated while reading), scientific notation (`1e6`)
  and `.var(name)` as an alternative to `@name` (#4).

* Step arguments in YAML accept nested calls as
  `{call: {name: fn, args: [...], kwargs: {...}}}` (#4).

* Derived series in YAML can be written as a single `expr` string instead of
  `steps`, e.g. `expr: "jsa(x, method = 'uniform') / 1000"`. The first
  argument of each call is the piped input, so an `expr` series is identical
  to the equivalent `steps` series (#12). A calculation on series on the right
  of an operator, such as `a / (b + c)`, is not supported yet; define it as its
  own series instead (#22).

* Raw series can be loaded from their `source` with `loaders`, a named list
  of functions keyed by source type, in `evaluate_pipeline()`,
  `execute_pipeline()`, `plan_pipeline()` and `tar_tsflow()`. A series in
  `inputs` takes precedence over its loader, and `inputs` is now optional
  (#11).

* Missing raw inputs no longer stop a run: a raw series that neither `inputs`
  nor a loader provides (or whose loader returns `NULL`) is skipped together
  with everything depending on it, with a warning, in both error modes. They
  are listed in `result$missing_inputs` and have status `"missing_input"`.
  `plan_pipeline()` reports raw series without a loader in `needs_inputs`
  (#11).

* `evaluate_pipeline()` accepts a `PipelinePlan` from `plan_pipeline()`, so a
  pipeline can be run exactly as it was inspected, reusing the plan's
  dependency graph and planned outputs (#6).

## Breaking changes

* Transforms are now passed as an explicit registry instead of being looked up
  in the calling environment. `transform_env` is replaced by `transforms`, a
  named list of functions, in `validate_pipeline()`, `build_dag()`,
  `plan_pipeline()`, `evaluate_pipeline()`, `execute_pipeline()`,
  `expand_node_result()` and `resolve_transform()`. Bare transform names must
  be registered; namespaced `pkg::fn` transforms work without registration
  (#5).

* `run_pipeline()` is removed; use `execute_pipeline()`, which it was an
  alias for (#6).

* Internal helpers are no longer exported: `normalize_series()`,
  `normalize_source()`, `normalize_steps()`, `normalize_step_arg()` (use
  `normalize_pipeline()`), `read_pipeline_csv()` (use `read_pipeline()`, which
  reads CSV by file extension), `extract_dependencies()` (use
  `upstream_nodes()` / `dag_edges()`) and `resolve_transform()` (#10).

* In-memory execution no longer runs DAG levels in parallel through
  `future` / `future.apply`; series are evaluated one after another, with one
  code path for both error modes. For parallel execution use `tar_tsflow()`
  with a `targets` parallel backend. `future` and `future.apply` are no longer
  suggested (#7).

* Evaluation hooks are passed explicitly as
  `hooks = list(binary_op = , postprocess = )` instead of being discovered by
  the names `tsflow_evaluate_binary_op` and `tsflow_postprocess_value` (#5).

* Series names must now be valid R names that start with a letter and contain
  no `..` (e.g. `gdp.real` or `gdp_real`, not `gdp-real`, `2020_gdp` or
  `.hidden`). `validate_pipeline()` lists all invalid names with suggested
  replacements (#17).

## Bug fixes

* Arithmetic steps between two series now fail with a clear message, naming
  the series and their periods, when the series don't overlap (previously a
  cryptic `ts` error, or a silently empty `xts` result), when `ts` series have
  different frequencies, and when a `ts` is combined with an `xts`
  (previously R ignored the dates and combined values by position).
  Partly overlapping series still combine on their common period; this is
  now documented and tested (#9).

* `expand_node_result()` replays arithmetic steps with the same arithmetic as
  evaluation, including the `binary_op` hook and the alignment checks.

* Error and warning messages no longer evaluate `{...}` in series names,
  references or other values from a pipeline file as R code; braces are now
  printed literally (#1).

* Unknown series references coming from step arguments or op operands now
  include a hint to use `{literal: ...}` for text values (#3).

* A pipe step that returns `NULL` no longer shifts the next transform's
  arguments: the `NULL` is passed as the first argument like any other piped
  value (#2).

# tsflow 0.1.1

* Initial release.
