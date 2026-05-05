test_that("build_dag returns a stable dependency plan", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(name = "raw_b", source = list(type = "file", path = "b.csv")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())

  expect_s3_class(dag, "DAGPlan")
  expect_equal(dag$dependencies$raw_a, character())
  expect_equal(dag$dependencies$raw_b, character())
  expect_equal(dag$dependencies$mid_c, "raw_a")
  expect_equal(dag$dependencies$final_d, c("mid_c", "raw_b"))
  expect_equal(dag$order, c("raw_a", "raw_b", "mid_c", "final_d"))
})

test_that("build_dag rejects cycles", {
  spec <- normalize_pipeline(list(
    series = list(
      list(
        name = "series_a",
        steps = list(
          list(input = "series_b")
        )
      ),
      list(
        name = "series_b",
        steps = list(
          list(input = "series_a")
        )
      )
    )
  ))

  expect_error(
    build_dag(spec),
    "dependency graph contains a cycle"
  )
})

test_that("build_dag can retain cycle nodes for visualization when requested", {
  spec <- normalize_pipeline(list(
    series = list(
      list(
        name = "series_a",
        steps = list(
          list(input = "series_b")
        )
      ),
      list(
        name = "series_b",
        steps = list(
          list(input = "series_a")
        )
      ),
      list(name = "raw_c", source = list(type = "file", path = "c.csv"))
    )
  ))

  dag <- build_dag(spec, allow_cycles = TRUE)

  expect_false(dag$is_acyclic)
  expect_equal(dag$cycle_nodes, c("series_a", "series_b"))
  expect_equal(dag$order, c("raw_c", "series_a", "series_b"))
  expect_equal(dag$node_status[dag$cycle_nodes], c(series_a = "cycle", series_b = "cycle"))

  sub <- dag_subgraph(dag, nodes = dag$cycle_nodes)
  expect_equal(sub$nodes, c("series_a", "series_b"))
  expect_equal(sub$edges$from, c("series_b", "series_a"))
  expect_equal(sub$edges$to, c("series_a", "series_b"))
})

test_that("build_dag includes downstream adjacency", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      ),
      list(
        name = "final_c",
        steps = list(
          list(input = "mid_b")
        )
      )
    )
  ))

  dag <- build_dag(spec)

  expect_equal(dag$downstream$raw_a, "mid_b")
  expect_equal(dag$downstream$mid_b, "final_c")
  expect_equal(dag$downstream$final_c, character())
})

test_that("upstream_nodes returns ordered ancestors", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(name = "raw_b", source = list(type = "file", path = "b.csv")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())

  expect_equal(upstream_nodes(dag, "final_d"), c("raw_a", "raw_b", "mid_c"))
  expect_equal(upstream_nodes(dag, "final_d", include_self = TRUE), c("raw_a", "raw_b", "mid_c", "final_d"))
})

test_that("downstream_nodes returns ordered descendants", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      ),
      list(
        name = "final_c",
        steps = list(
          list(input = "mid_b")
        )
      )
    )
  ))

  dag <- build_dag(spec)

  expect_equal(downstream_nodes(dag, "raw_a"), c("mid_b", "final_c"))
  expect_equal(downstream_nodes(dag, "raw_a", include_self = TRUE), c("raw_a", "mid_b", "final_c"))
})

test_that("dag_edges exports dependency edges", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(name = "raw_b", source = list(type = "file", path = "b.csv")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())
  edges <- dag_edges(dag)

  expect_equal(names(edges), c("from", "to"))
  expect_equal(nrow(edges), 3)
  expect_equal(edges$from, c("raw_a", "mid_c", "raw_b"))
  expect_equal(edges$to, c("mid_c", "final_d", "final_d"))
})

test_that("dag_edges can include edge labels", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "injected")),
      list(name = "raw_b", source = list(type = "injected")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())
  edges <- dag_edges(dag, include_labels = TRUE)

  expect_equal(names(edges), c("from", "to", "label"))
  expect_equal(edges$label, c("scale: x", "join: x", "join: y"))
})

test_that("DAG helpers reject unknown nodes", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv"))
    )
  ))

  dag <- build_dag(spec)

  expect_error(upstream_nodes(dag, "missing"), "node `missing` is not present in the DAG")
  expect_error(downstream_nodes(dag, "missing"), "node `missing` is not present in the DAG")
})

test_that("dag_subgraph returns focused nodes and edges", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(name = "raw_b", source = list(type = "file", path = "b.csv")),
      list(
        name = "mid_c",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      ),
      list(
        name = "final_d",
        steps = list(
          list(input = "mid_c"),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())
  sub <- dag_subgraph(dag, upstream_of = "final_d")

  expect_s3_class(sub, "DAGSubgraph")
  expect_equal(sub$nodes, c("raw_a", "raw_b", "mid_c", "final_d"))
  expect_equal(sub$edges$from, c("raw_a", "mid_c", "raw_b"))
  expect_equal(sub$edges$to, c("mid_c", "final_d", "final_d"))
})

test_that("expand_node_dag disentangles one node into ordered pipe steps", {
  scale <- function(x, factor) x * factor
  join <- function(x, y) x + y

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(name = "raw_b", source = list(type = "file", path = "b.csv")),
      list(
        name = "final_d",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100))),
          list(call = list(name = "join", args = list("raw_b")))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())
  expanded <- expand_node_dag(dag, "final_d")

  expect_s3_class(expanded, "ExpandedNodeDAG")
  expect_equal(
    expanded$nodes,
    c("raw_a", "final_d::step_1", "final_d::step_2", "final_d")
  )
  expect_equal(
    expanded$edges$from,
    c("raw_a", "final_d::step_1", "final_d::step_2")
  )
  expect_equal(
    expanded$edges$to,
    c("final_d::step_1", "final_d::step_2", "final_d")
  )
  expect_equal(expanded$labels[["final_d::step_1"]], "scale(100)")
  expect_equal(expanded$labels[["final_d::step_2"]], "join(raw_b)")
})

test_that("dag_to_dot exports Graphviz DOT", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))

  dag <- build_dag(spec)
  dot <- dag_to_dot(dag, graph_name = "demo", rankdir = "TB")

  expect_match(dot, 'digraph "demo" \\{')
  expect_match(dot, "rankdir=TB;")
  expect_match(dot, '"raw_a" -> "mid_b";')
})

test_that("dag_to_igraph exports an igraph object when igraph is installed", {
  testthat::skip_if_not_installed("igraph")

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))

  dag <- build_dag(spec)
  graph <- dag_to_igraph(dag)

  expect_s3_class(graph, "igraph")
  expect_true(igraph::vcount(graph) == 2)
  expect_true(igraph::ecount(graph) == 1)
  expect_equal(igraph::E(graph)$label, "input")
})

test_that("dag_to_visnetwork exports a visNetwork widget when dependencies are installed", {
  testthat::skip_if_not_installed("igraph")
  testthat::skip_if_not_installed("visNetwork")

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))

  dag <- build_dag(spec)
  widget <- dag_to_visnetwork(dag)

  expect_s3_class(widget, "visNetwork")
  expect_s3_class(widget, "htmlwidget")
})

test_that("dag_to_visnetwork preserves cycle node annotations", {
  testthat::skip_if_not_installed("igraph")
  testthat::skip_if_not_installed("visNetwork")

  spec <- normalize_pipeline(list(
    series = list(
      list(
        name = "series_a",
        steps = list(
          list(input = "series_b")
        )
      ),
      list(
        name = "series_b",
        steps = list(
          list(input = "series_a")
        )
      )
    )
  ))

  dag <- build_dag(spec, allow_cycles = TRUE)
  widget <- dag_to_visnetwork(dag, nodes = dag$cycle_nodes)

  expect_s3_class(widget, "visNetwork")
  expect_equal(widget$x$nodes$status, c("cycle", "cycle"))
  expect_equal(widget$x$nodes$color.background, c("#FEF3C7", "#FEF3C7"))
})

test_that("dag_to_visnetwork renders an expanded node graph", {
  testthat::skip_if_not_installed("igraph")
  testthat::skip_if_not_installed("visNetwork")

  scale <- function(x, factor) x * factor

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a"),
          list(call = list(name = "scale", args = list(100)))
        )
      )
    )
  ))

  dag <- build_dag(spec, transform_env = environment())
  widget <- dag_to_visnetwork(expand_node_dag(dag, "mid_b"))

  expect_s3_class(widget, "visNetwork")
  expect_s3_class(widget, "htmlwidget")
})

test_that("dag_to_visnetwork accepts a PipelinePlan directly", {
  testthat::skip_if_not_installed("igraph")
  testthat::skip_if_not_installed("visNetwork")

  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))

  plan <- structure(
    list(
      parsed = list(),
      normalized = spec,
      dag = build_dag(spec)
    ),
    class = c("PipelinePlan", "tsflow_spec")
  )

  widget <- dag_to_visnetwork(plan)

  expect_s3_class(widget, "visNetwork")
  expect_s3_class(widget, "htmlwidget")
})

test_that("PipelineResult stores failed and skipped node status on result$dag", {
  result <- structure(
    list(
      plan = structure(
        list(
          nodes = c("raw_a", "broken_b", "skipped_c", "healthy_d"),
          order = c("raw_a", "broken_b", "skipped_c", "healthy_d"),
          dependencies = list(
            raw_a = character(),
            broken_b = "raw_a",
            skipped_c = "broken_b",
            healthy_d = character()
          ),
          downstream = list(
            raw_a = "broken_b",
            broken_b = "skipped_c",
            skipped_c = character(),
            healthy_d = character()
          ),
          edges = list(
            raw_a = "broken_b",
            broken_b = "skipped_c",
            skipped_c = character(),
            healthy_d = character()
          ),
          edge_labels = list()
        ),
        class = c("DAGPlan", "tsflow_spec")
      ),
      dag = annotate_result_dag(
        structure(
          list(
            nodes = c("raw_a", "broken_b", "skipped_c", "healthy_d"),
            order = c("raw_a", "broken_b", "skipped_c", "healthy_d"),
            dependencies = list(
              raw_a = character(),
              broken_b = "raw_a",
              skipped_c = "broken_b",
              healthy_d = character()
            ),
            downstream = list(
              raw_a = "broken_b",
              broken_b = "skipped_c",
              skipped_c = character(),
              healthy_d = character()
            ),
            edges = list(
              raw_a = "broken_b",
              broken_b = "skipped_c",
              skipped_c = character(),
              healthy_d = character()
            ),
            edge_labels = list()
          ),
          class = c("DAGPlan", "tsflow_spec")
        ),
        errors = list(broken_b = "boom"),
        skipped = "skipped_c"
      ),
      errors = list(broken_b = "boom"),
      skipped = "skipped_c"
    ),
    class = c("PipelineResult", "tsflow_spec")
  )

  dag <- result$dag

  expect_equal(dag$node_status[["raw_a"]], "ok")
  expect_equal(dag$node_status[["broken_b"]], "failed")
  expect_equal(dag$node_status[["skipped_c"]], "skipped")
  expect_equal(dag$node_status[["healthy_d"]], "ok")
})

test_that("dag_to_visnetwork accepts a PipelineResult directly", {
  testthat::skip_if_not_installed("igraph")
  testthat::skip_if_not_installed("visNetwork")

  dag <- structure(
    list(
      nodes = c("raw_a", "mid_b"),
      order = c("raw_a", "mid_b"),
      dependencies = list(
        raw_a = character(),
        mid_b = "raw_a"
      ),
      downstream = list(
        raw_a = "mid_b",
        mid_b = character()
      ),
      edge_labels = list(
        raw_a = setNames(character(), character()),
        mid_b = c(raw_a = "input")
      ),
      node_kind = c(raw_a = "raw", mid_b = "derived"),
      node_role = c(raw_a = "raw", mid_b = "alias"),
      node_pipeline = list(
        raw_a = list(input = "raw_a", steps = character()),
        mid_b = list(input = "raw_a", steps = "pass-through")
      )
    ),
    class = c("DAGPlan", "tsflow_spec")
  )

  result <- structure(
    list(
      plan = dag,
      dag = annotate_result_dag(dag, errors = list(), skipped = character()),
      values = list(),
      outputs = list(),
      errors = list(),
      skipped = character(),
      error_mode = "fail"
    ),
    class = c("PipelineResult", "tsflow_spec")
  )

  widget <- dag_to_visnetwork(result)

  expect_s3_class(widget, "visNetwork")
  expect_s3_class(widget, "htmlwidget")
})

test_that("write_dag_dot writes DOT text to disk", {
  spec <- normalize_pipeline(list(
    series = list(
      list(name = "raw_a", source = list(type = "file", path = "a.csv")),
      list(
        name = "mid_b",
        steps = list(
          list(input = "raw_a")
        )
      )
    )
  ))

  dag <- build_dag(spec)
  path <- tempfile(fileext = ".dot")

  written <- write_dag_dot(dag, path)

  expect_equal(written, path)
  expect_true(file.exists(path))
  expect_match(paste(readLines(path), collapse = "\n"), '"raw_a" -> "mid_b";')
})
