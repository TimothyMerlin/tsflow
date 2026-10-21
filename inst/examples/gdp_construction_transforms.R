# Transforms used by the gdp_construction example pipeline (see
# vignette("gdp-construction")). Requires the tempdisagg and zoo packages.

jsa <- function(
  ts_sa,
  ts_nsa,
  method = "uniform",
  conversion = "sum"
) {
    ts_nsa_a <- tempdisagg::ta(ts_nsa, conversion = conversion, to = "annual")

    if (method == "denton-cholette") {
        return(tempdisagg::td(
            ts_nsa_a ~ 0 + ts_sa,
            method = "denton-cholette",
            criterion = "additive",
            h = 1
        )$values)
    }

    tempdisagg::td(
        ts_nsa_a ~ ts_sa,
        conversion = conversion,
        to = "quarterly",
        method = method
    )$values
}

real_gr_nom <- function(ts_gr, ts_nom, base_year) {
    ts_gr <- zoo::na.trim(ts_gr)
    if (frequency(ts_gr) < frequency(ts_nom)) {
        ts_nom <- tempdisagg::ta(
            ts_nom,
            conversion = "sum",
            to = frequency(ts_gr)
        )
    }

    ts_real <- ts(
        NA,
        start = start(ts_gr) - c(1, 0),
        end = end(ts_gr),
        frequency = frequency(ts_gr)
    )

    base_year_value <- as.numeric(window(
        ts_nom,
        start = c(base_year, 1),
        end = c(base_year, 1)
    ))
    window(ts_real, start = c(base_year, 1), end = c(base_year, 1)) <- base_year_value

    product_up <- (ts_gr / 100 + 1)
    if (base_year < end(ts_gr)[1]) {
        up_vals <- as.numeric(window(product_up, start = c(base_year + 1, 1)))
        window(ts_real, start = c(base_year + 1, 1)) <- base_year_value * cumprod(up_vals)
    }

    product_down <- 1 / product_up
    if (base_year > start(ts_gr)[1]) {
        down_vals <- as.numeric(window(product_down, end = c(base_year, 1)))
        window(ts_real, end = c(base_year - 1, 1)) <- rev(base_year_value * cumprod(rev(down_vals)))
    }

    ts_real
}

a2q_sum <- function(ts_d, indicator, conversion = "sum") {
    ts_d <- zoo::na.trim(ts_d)
    indicator <- zoo::na.trim(indicator)

    start_td <- max(stats::time(indicator)[1], stats::time(ts_d)[1])
    end_td <- utils::tail(stats::time(indicator), 1)

    indicator <- window(indicator, start = start_td, end = end_td)

    if (any(!is.na(ts_d)) && any(!is.na(indicator))) {
        return(tempdisagg::td(
            ts_d ~ indicator,
            conversion = conversion,
            to = "quarterly",
            method = "chow-lin-maxlog"
        )$values)
    }

    tempdisagg::td(
        ts_d ~ 1,
        conversion = conversion,
        to = "quarterly",
        method = "denton-cholette"
    )$values
}
