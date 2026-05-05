set.seed(42)

gdp_q_trend <- seq(250, 310, length.out = 40)
gdp.nominal.quarterly.sa <- ts(
  round(gdp_q_trend + rnorm(40, 0, 3), 1),
  start = c(2010, 1), frequency = 4
)

gdp.nominal.annual.nsa <- ts(
  c(1015, 1040, 1065, 1085, 1100, 1120, 1145, 1165, 1180, 1205),
  start = 2010, frequency = 1
)

construction.nominal.annual <- ts(
  c(120000, 124500, 129000, 134200, 138000, 140500, 144000, 148500, 152000, 155500),
  start = 2010, frequency = 1
)

construction.growth.annual <- ts(
  c(2.1, 3.5, 2.8, 1.2, -0.5, 4.1, 3.0, 2.2, 1.8, 3.3),
  start = 2010, frequency = 1
)

gdp_construction_inputs <- list(
  "gdp.nominal.quarterly.sa"    = gdp.nominal.quarterly.sa,
  "gdp.nominal.annual.nsa"      = gdp.nominal.annual.nsa,
  "construction.nominal.annual" = construction.nominal.annual,
  "construction.growth.annual"  = construction.growth.annual
)

if (!dir.exists("data")) dir.create("data")

save(
  gdp_construction_inputs,
  file = file.path("data", "gdp_construction_inputs.rda"),
  compress = "bzip2"
)
