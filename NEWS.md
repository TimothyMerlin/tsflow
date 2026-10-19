# tsflow (development version)

* New `series_names()` returns the series names declared by a pipeline in
  definition order, with an optional `kind` filter (`"raw"` / `"derived"`). It
  accepts a `PipelineSpec`, `PipelinePlan`, or `PipelineResult`.

## Breaking changes

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
