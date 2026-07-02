#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
"_PACKAGE"

## usethis namespace: start
#' @importFrom stats approx cor dt median optim optimize pt quantile rnorm
#' @importFrom stats rt runif sd var rchisq setNames lm coef
## usethis namespace: end
NULL

#' Null-coalescing operator
#' @param x Left-hand value.
#' @param y Default value if x is NULL.
#' @return x if not NULL, otherwise y.
#' @keywords internal
#' @noRd
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Evaluate an expression with a temporary RNG seed
#' @param seed Integer seed.
#' @param expr Expression to evaluate.
#' @return The value of \code{expr}.
#' @keywords internal
.with_local_seed <- function(seed, expr) {
  expr <- substitute(expr)
  had_seed <- exists(".Random.seed", envir = globalenv(), inherits = FALSE)
  if (had_seed) {
    old_seed <- get(".Random.seed", envir = globalenv(), inherits = FALSE)
  }

  on.exit({
    if (had_seed) {
      assign(".Random.seed", old_seed, envir = globalenv())
    } else if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
      rm(".Random.seed", envir = globalenv())
    }
  }, add = TRUE)

  set.seed(as.integer(seed)[1L])
  eval.parent(expr)
}
