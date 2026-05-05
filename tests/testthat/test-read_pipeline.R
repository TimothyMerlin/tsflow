test_that("read_pipeline reads YAML into an R list", {
  path <- tempfile(fileext = ".yml")
  writeLines(
    c(
      "series:",
      "  - name: raw_a",
      "    source:",
      "      type: internal_api",
      "      key: raw_a"
    ),
    con = path
  )

  parsed <- read_pipeline(path)

  expect_type(parsed, "list")
  expect_equal(parsed$series[[1]]$name, "raw_a")
  expect_equal(parsed$series[[1]]$source$type, "internal_api")
})

test_that("write_pipeline writes a reusable YAML pipeline", {
  dir <- tempfile()
  dir.create(dir)
  csv_path <- file.path(dir, "pipeline.csv")
  yaml_path <- file.path(dir, "pipeline.compiled.yml")
  meta_path <- file.path(dir, "pipeline.meta.yml")

  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"scale(@factor)\""
    ),
    con = csv_path
  )
  writeLines(
    c(
      "vars:",
      "  factor: 100",
      "outputs:",
      "  - derived_a",
      "metadata:",
      "  owner: kof"
    ),
    con = meta_path
  )

  parsed <- read_pipeline(csv_path)
  out <- write_pipeline(parsed, yaml_path)
  reparsed <- read_pipeline(yaml_path)

  expect_equal(out, yaml_path)
  expect_true(file.exists(yaml_path))
  expect_equal(reparsed, parsed)
})

test_that("read_pipeline merges sibling sidecar metadata", {
  dir <- tempfile()
  dir.create(dir)
  path <- file.path(dir, "pipeline.csv")
  meta_path <- file.path(dir, "pipeline.meta.yml")

  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"scale(@factor)\""
    ),
    con = path
  )
  writeLines(
    c(
      "vars:",
      "  factor: 100",
      "outputs:",
      "  - derived_a",
      "metadata:",
      "  owner: kof"
    ),
    con = meta_path
  )

  parsed <- read_pipeline(path)

  expect_equal(parsed$vars$factor, 100)
  expect_equal(parsed$outputs, "derived_a")
  expect_equal(parsed$metadata$owner, "kof")
  expect_equal(parsed$series[[2]]$steps[[2]]$call$args[[1]]$var, "factor")
})

test_that("read_pipeline preserves vector vars from the sidecar", {
  dir <- tempfile()
  dir.create(dir)
  path <- file.path(dir, "pipeline.csv")
  meta_path <- file.path(dir, "pipeline.meta.yml")

  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"a2q_sum(end=@cutoff)\""
    ),
    con = path
  )
  writeLines(
    c(
      "vars:",
      "  cutoff:",
      "    - 2026",
      "    - 4"
    ),
    con = meta_path
  )

  parsed <- read_pipeline(path)
  call <- parsed$series[[2]]$steps[[2]]$call

  expect_equal(parsed$vars$cutoff, c(2026, 4))
  expect_equal(call$kwargs$end$var, "cutoff")
})

test_that("read_pipeline reads wide CSV mappings into pipeline shape", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1,step_2",
      "derived_a,raw_a,scale(100),",
      "derived_b,derived_a,div(raw_b),mul(10)"
    ),
    con = path
  )

  parsed <- read_pipeline(path)

  expect_type(parsed, "list")
  expect_equal(vapply(parsed$series, `[[`, character(1), "name"), c("raw_a", "raw_b", "derived_a", "derived_b"))
  expect_equal(parsed$series[[1]]$source$type, "injected")
  expect_equal(parsed$series[[3]]$steps[[1]]$input, "raw_a")
  expect_equal(parsed$series[[3]]$steps[[2]]$call$name, "scale")
  expect_equal(parsed$series[[3]]$steps[[2]]$call$args[[1]], 100)
  expect_equal(parsed$series[[4]]$steps[[2]]$op$type, "div")
  expect_equal(parsed$series[[4]]$steps[[2]]$op$right, "raw_b")
})

test_that("read_pipeline_csv supports kwargs and quoted string literals", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"jsa(raw_b, method='denton-cholette', conversion='sum')\""
    ),
    con = path
  )

  parsed <- read_pipeline_csv(path)
  call <- parsed$series[[3]]$steps[[2]]$call

  expect_equal(call$name, "jsa")
  expect_equal(call$args[[1]], "raw_b")
  expect_equal(call$kwargs$method$literal, "denton-cholette")
  expect_equal(call$kwargs$conversion$literal, "sum")
})

test_that("read_pipeline_csv supports var placeholders", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"jsa(raw_b, method=@method, base_year=@base_year)\""
    ),
    con = path
  )

  parsed <- read_pipeline_csv(path)
  call <- parsed$series[[3]]$steps[[2]]$call

  expect_equal(call$args[[1]], "raw_b")
  expect_equal(call$kwargs$method$var, "method")
  expect_equal(call$kwargs$base_year$var, "base_year")
})

test_that("read_pipeline_csv compiles legacy fct and fct_chain formulas", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1,step_2",
      "derived_a,raw_a,\"fct(raw_a+raw_b/raw_c)\",",
      "derived_b,real_a,\"fct_chain(real_a+real_b-real_c, nom=nreal_a+nreal_b-nreal_c)\","
    ),
    con = path
  )

  parsed <- read_pipeline_csv(path)
  series_names <- vapply(parsed$series, `[[`, character(1), "name")
  call_fct <- parsed$series[[match("derived_a", series_names)]]$steps[[2]]$call
  call_chain <- parsed$series[[match("derived_b", series_names)]]$steps[[2]]$call

  expect_equal(call_fct$name, "fct_formula")
  expect_equal(call_fct$kwargs$formula$literal, "raw_a+raw_b/raw_c")
  expect_equal(call_fct$kwargs$tokens$literal, "raw_a,raw_b,raw_c")
  expect_equal(call_fct$kwargs$ops$literal, "+,+,/")
  expect_equal(call_chain$name, "fct_chain_formula")
  expect_equal(call_chain$kwargs$formula$literal, "real_a+real_b-real_c")
  expect_equal(call_chain$kwargs$nominal_formula$literal, "nreal_a+nreal_b-nreal_c")
  expect_equal(call_chain$kwargs$nominal_tokens$literal, "nreal_a,nreal_b,nreal_c")
  expect_equal(call_chain$kwargs$nominal_ops$literal, "+,+,-")
  expect_equal(call_chain$kwargs$nominal__nreal_b, "nreal_b")
  expect_equal(call_chain$kwargs$nominal__nreal_c, "nreal_c")
})

test_that("read_pipeline_csv parses fct_chain formulas with extra named args", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_b,real_a,\"fct_chain(real_a+real_b-real_c, nom=nreal_a+nreal_b-nreal_c, base_year=@base_year)\""
    ),
    con = path
  )
  writeLines(
    c(
      "vars:",
      "  base_year: 2022"
    ),
    con = sub("\\.csv$", ".meta.yml", path)
  )

  parsed <- read_pipeline(path)
  series_names <- vapply(parsed$series, `[[`, character(1), "name")
  call_chain <- parsed$series[[match("derived_b", series_names)]]$steps[[2]]$call

  expect_equal(call_chain$name, "fct_chain_formula")
  expect_equal(call_chain$kwargs$base_year$var, "base_year")
})

test_that("read_pipeline_csv rejects fct_chain without explicit nominal formula", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_b,real_a,\"fct_chain(real_a+real_b)\""
    ),
    con = path
  )

  expect_error(
    read_pipeline_csv(path),
    "must use `fct_chain\\(real_formula, nom=nominal_formula, \\.\\.\\.\\)`"
  )
})

test_that("read_pipeline_csv rejects unsupported operators in fct_chain formulas", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_b,real_a,\"fct_chain(real_a/real_b, nom=nreal_a+nreal_b)\""
    ),
    con = path
  )

  expect_error(
    read_pipeline_csv(path),
    "only supports `\\+` and `-` operators"
  )
})

test_that("read_pipeline_csv rejects invalid legacy formula operands", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_b,real_a,\"fct(real_a+1)\""
    ),
    con = path
  )

  expect_error(
    read_pipeline_csv(path),
    "contains invalid operands"
  )
})

test_that("CSV quoted string kwargs stay literals through normalization", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"jsa(raw_b, method='denton-cholette')\""
    ),
    con = path
  )

  spec <- normalize_pipeline(read_pipeline_csv(path))
  kwargs <- spec$series[[3]]$expression$call$kwargs

  expect_s3_class(kwargs$method, "LiteralNode")
  expect_equal(kwargs$method$value, "denton-cholette")
})

test_that("read_pipeline_csv does not treat quoted string kwargs as raw refs", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"jsa(raw_b, method='denton-cholette')\""
    ),
    con = path
  )

  parsed <- read_pipeline_csv(path)
  names <- vapply(parsed$series, `[[`, character(1), "name")

  expect_false("method='denton-cholette'" %in% names)
  expect_equal(names, c("raw_a", "raw_b", "derived_a"))
})

test_that("read_pipeline_csv rejects malformed step cells", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,raw_b / 100"
    ),
    con = path
  )

  expect_error(
    read_pipeline_csv(path),
    "must use the form"
  )
})

test_that("read_pipeline_csv rejects missing required headers", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,step_1",
      "derived_a,scale(100)"
    ),
    con = path
  )

  expect_error(
    read_pipeline_csv(path),
    "missing required columns: input"
  )
})
