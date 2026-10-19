# tsflow (development version)

* New `series_names()` returns the series names declared by a pipeline in
  definition order, with an optional `kind` filter (`"raw"` / `"derived"`). It
  accepts a `PipelineSpec`, `PipelinePlan`, or `PipelineResult`.

* New option `tsflow.allowed_packages` restricts which packages namespaced
  `pkg::fn` transforms may resolve from (#5).

* CSV step cells are now parsed with R's own parser (`str2lang()`) instead of
  a hand-written one. Existing cells read exactly as before, and cells can now
  use nested calls (`a2q_sum(jsa(raw_b), conversion = @c)`), arithmetic on
  numbers (`100 / 4`, evaluated while reading), scientific notation (`1e6`)
  and `.var(name)` as an alternative to `@name` (#4).

* Step arguments in YAML accept nested calls as
  `{call: {name: fn, args: [...], kwargs: {...}}}` (#4).

## Breaking changes

* Transforms are now passed as an explicit registry instead of being looked up
  in the calling environment. `transform_env` is replaced by `transforms`, a
  named list of functions, in `validate_pipeline()`, `build_dag()`,
  `plan_pipeline()`, `evaluate_pipeline()`, `execute_pipeline()`,
  `run_pipeline()`, `expand_node_result()` and `resolve_transform()`. Bare
  transform names must be registered; namespaced `pkg::fn` transforms work
  without registration (#5).

* Evaluation hooks are passed explicitly as
  `hooks = list(binary_op = , postprocess = )` instead of being discovered by
  the names `tsflow_evaluate_binary_op` and `tsflow_postprocess_value` (#5).

* Series names must now be valid R names that start with a letter and contain
  no `..` (e.g. `gdp.real` or `gdp_real`, not `gdp-real`, `2020_gdp` or
  `.hidden`). `validate_pipeline()` lists all invalid names with suggested
  replacements (#17).

## Bug fixes

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
