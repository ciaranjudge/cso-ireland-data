## ---- include = FALSE---------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup--------------------------------------------------------------------
# # Install or update the package:
# install.packages("csodata")

library(csodata)

## -----------------------------------------------------------------------------
toc <- cso_get_toc()
head(toc)

## -----------------------------------------------------------------------------
tbl1 <- cso_get_data("PEA19")

## -----------------------------------------------------------------------------
meta1 <- cso_get_meta("CDP06")
cso_disp_meta("CDP06")

## -----------------------------------------------------------------------------
shp <- cso_get_geo("NUTS3_2011")

## ---- fig.width = 5, fig.height=6---------------------------------------------
# install.packages("tmap")
library(tmap)

t <- tm_shape(shp) +
       tm_fill(col="TOTAL2011", 
                    palette = viridisLite::viridis(20),
                    style="cont", legend.reverse = TRUE,
                    title = "Population 2011") +
       tm_borders(col = "black") +
       tm_layout(frame = FALSE, scale = 1.3)
t

## -----------------------------------------------------------------------------
cso_clear_cache()

