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

parse_step <- function(cell, input_name = "raw_a") {
  parse_csv_step(cell, input_name = input_name)
}

test_that("CSV @var references are rewritten only outside quotes and at token starts", {
  step <- parse_step("f(x, label = 'email@example.com', m = @m, n = @n.1)")$step$call
  expect_equal(step$kwargs$label$literal, "email@example.com")
  expect_equal(step$kwargs$m$var, "m")
  expect_equal(step$kwargs$n$var, "n.1")

  expect_error(parse_step("f(x@y)"), "unsupported argument `x@y`", fixed = TRUE)
})

test_that("CSV cells accept .var() directly", {
  step <- parse_step("jsa(raw_b, method = .var(jsa_method))")$step$call
  expect_equal(step$kwargs$method$var, "jsa_method")

  expect_error(parse_step("f(x, m = .var(a, b))"), "`.var(name)` with a single var name", fixed = TRUE)
  expect_error(parse_step(".var(a)"), "must use the form", fixed = TRUE)
})

test_that("CSV cells support nested calls", {
  parsed <- parse_step("a2q_sum(jsa(raw_b, method = @m), indicator = stats::lag(raw_c, -1))")
  call <- parsed$step$call

  expect_equal(call$name, "a2q_sum")
  expect_equal(call$args[[1]]$call$name, "jsa")
  expect_equal(call$args[[1]]$call$args[[1]], "raw_b")
  expect_equal(call$args[[1]]$call$kwargs$method$var, "m")
  expect_equal(call$kwargs$indicator$call$name, "stats::lag")
  expect_equal(call$kwargs$indicator$call$args[[2]], -1)
  expect_equal(parsed$refs, c("raw_b", "raw_c"))
})

test_that("CSV cells fold arithmetic on numbers and accept scientific notation", {
  call <- parse_step("scale(x, factor = 100 / 4, offset = -(2 + 3), big = 1e6, n = 5L)")$step$call

  expect_equal(call$kwargs$factor, 25)
  expect_equal(call$kwargs$offset, -5)
  expect_equal(call$kwargs$big, 1e6)
  expect_identical(call$kwargs$n, 5L)
  expect_equal(parse_step("div(1e3)")$step$op$right, 1000)
})

test_that("CSV cells reject arithmetic on series refs and vars inside arguments", {
  expect_error(
    parse_step("scale(x, factor = raw_b * 2)"),
    "arithmetic on `raw_b`; arithmetic inside arguments only supports numbers",
    fixed = TRUE
  )
  expect_error(parse_step("scale(x, factor = @f * 2)"), "arithmetic on `.var(f)`", fixed = TRUE)
})

test_that("CSV cells reject unsupported syntax with clear errors", {
  expect_error(parse_step("f(x[1])"), "unsupported argument `x[1]`", fixed = TRUE)
  expect_error(parse_step("f(x$y)"), "unsupported argument `x$y`", fixed = TRUE)
  expect_error(parse_step("f(function(x) x)"), "unsupported argument", fixed = TRUE)
  expect_error(parse_step("f(x, NA)"), "missing value (`NA`)", fixed = TRUE)
  expect_error(parse_step("f(x, NULL)"), "`NULL`, which is not supported", fixed = TRUE)
  expect_error(parse_step("f(x, )"), "contains an empty argument", fixed = TRUE)
  expect_error(parse_step("f(x, 'open)"), "is not a valid expression", fixed = TRUE)
  expect_error(parse_step("x[1]"), "must use the form", fixed = TRUE)
  expect_error(parse_step("f(mul(x))"), "unsupported argument `mul(x)`", fixed = TRUE)
})

test_that("CSV nested calls infer raw series and evaluate end to end", {
  path <- tempfile(fileext = ".csv")
  writeLines(
    c(
      "name,input,step_1",
      "derived_a,raw_a,\"add_ts(scale(raw_b, factor = 100 / 4))\""
    ),
    con = path
  )

  parsed <- read_pipeline(path)
  expect_equal(vapply(parsed$series, `[[`, character(1), "name"), c("raw_a", "raw_b", "derived_a"))

  scale <- function(x, factor) x * factor
  add_ts <- function(x, y) x + y
  spec <- normalize_pipeline(parsed)
  result <- evaluate_pipeline(
    spec,
    inputs = list(raw_a = ts(1:3), raw_b = ts(c(1, 1, 1))),
    transforms = list(scale = scale, add_ts = add_ts),
    progress = FALSE
  )

  expect_equal(as.numeric(result$values$derived_a), c(26, 27, 28))
  expect_equal(build_dag(spec, transforms = list(scale = scale, add_ts = add_ts))$dependencies$derived_a, c("raw_a", "raw_b"))
})

test_that("YAML step args accept nested calls", {
  spec <- normalize_pipeline(list(series = list(
    list(name = "raw_a", source = list(type = "injected")),
    list(name = "raw_b", source = list(type = "injected")),
    list(name = "derived", steps = list(
      list(input = "raw_a"),
      list(call = list(name = "add_ts", args = list(
        list(call = list(name = "scale", args = list("raw_b"), kwargs = list(factor = 2)))
      )))
    ))
  )))

  nested <- spec$series[[3]]$expression$call$args[[1]]
  expect_s3_class(nested, "CallNode")
  expect_equal(nested$name, "scale")
  expect_equal(extract_dependencies(spec$series[[3]]$expression), c("raw_a", "raw_b"))
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
