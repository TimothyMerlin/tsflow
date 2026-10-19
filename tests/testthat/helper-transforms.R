# Collect the transform functions a test defines locally into a registry.
local_transforms <- function(env = parent.frame()) {
  objects <- mget(ls(env), envir = env)
  objects[vapply(objects, is.function, logical(1))]
}
