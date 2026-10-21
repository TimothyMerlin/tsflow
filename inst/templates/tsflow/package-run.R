# Run from the package root. For an installed package, use
# {{package}}::run_pipeline() directly instead of load_all().
pkgload::load_all(".")

result <- {{package}}::run_pipeline()
result$values$adjusted
result$errors
result$missing_inputs

# Optional caching: supply store = "_tsflow" to run_pipeline().
# See the generated setup guide for targets and the next development steps.
