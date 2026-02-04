cache_path <- function(name, cfg = CFG) here::here("cache", paste0(name, ".rds"))
meta_path  <- function(name, cfg = CFG) here::here("cache", "_meta", paste0(name, ".json"))

read_cache_fingerprint <- function(name, cfg = CFG) {
  meta <- meta_path(name, cfg)
  if (!file.exists(meta)) return(NULL)
  m <- jsonlite::read_json(meta, simplifyVector = TRUE)
  m$fingerprint
}

deps_code_mtime <- function(paths) {
  info <- file.info(paths)
  setNames(as.character(info$mtime), paths)
}

deps_file_mtime <- function(path) {
  as.character(file.info(path)$mtime)
}

fingerprint <- function(deps) digest::digest(deps, algo = "xxhash64")

cache_or_compute <- function(name, compute_fn, deps, force = FALSE, cfg = CFG) {
  stopifnot(is.function(compute_fn))
  fp <- fingerprint(deps)

  rds <- cache_path(name, cfg)
  meta <- meta_path(name, cfg)

  if (!isTRUE(cfg$use_cache)) {
    message("[compute] ", name, " (cache disabled)")
    return(compute_fn())
  }

  if (cfg$use_cache && file.exists(rds) && file.exists(meta) && !force) {
    m <- jsonlite::read_json(meta, simplifyVector = TRUE)
    if (!is.null(m$fingerprint) && identical(m$fingerprint, fp)) {
      message("[cache hit] ", name)
      return(readRDS(rds))
    }
    message("[cache stale] ", name)
  } else if (cfg$use_cache && file.exists(rds) && !force) {
    message("[cache missing meta] ", name)
  }

  message("[compute] ", name)
  obj <- compute_fn()
  saveRDS(obj, rds)
  jsonlite::write_json(
    list(
      name = name,
      fingerprint = fp,
      created_at = as.character(Sys.time()),
      deps = deps
    ),
    meta,
    pretty = TRUE, auto_unbox = TRUE
  )
  obj
}
