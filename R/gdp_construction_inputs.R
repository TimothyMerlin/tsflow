#' Example Inputs for the GDP and Construction Pipeline
#'
#' Synthetic annual and quarterly time series used by the GDP and construction
#' example pipeline and vignette. The object is a named list keyed by series
#' name so it can be passed directly to `execute_pipeline()` as `inputs`.
#'
#' @format A named list of `ts` objects:
#' \describe{
#'   \item{gdp.nominal.quarterly.sa}{Quarterly nominal GDP, seasonally adjusted.}
#'   \item{gdp.nominal.annual.nsa}{Annual nominal GDP, not seasonally adjusted.}
#'   \item{construction.nominal.annual}{Annual nominal construction spending (millions).}
#'   \item{construction.growth.annual}{Annual real construction growth rate (percent).}
#' }
#' @usage data(gdp_construction_inputs)
"gdp_construction_inputs"
