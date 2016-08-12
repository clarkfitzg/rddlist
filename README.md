# rddlist

Implements distributed computation on an R list with a Spark backend.  This
allows an R programmer to use familiar operations like `[[`, `lapply`, and
`mapply` from within R to perform computation on larger data sets using a
Spark cluster. This is a powerful combination, as any data in R can be
represented with a list, and `*apply` operations allow the application of
arbitrary user defined functions, provided they are pure. So this should
work well for embarrassingly parallel problems.

The main purpose of this project is to serve as an object that will connect
Spark to the more general [ddR project](https://github.com/vertica/ddR)
(distributed data in R).  Work supported by
[R-Consortium](https://www.r-consortium.org/projects).

Under the hood this works by serializing each element of the list into a
byte array and storing it in a Spark Pair RDD (resilient distributed data
set).

## Examples

Sparkapi provides the spark connections.

```R
library(sparkapi)
library(rddlist)
sc <- start_shell(master = "local")

x <- list(1:10, letters, rnorm(10))
xrdd <- rddlist(sc, x)
```

`xrdd` is an object in the local R session referencing the actual data
residing in Spark.
Collecting deserializes the object from Spark into local R.

```R
x2 <- collect(xrdd)
identical(x, x2)
# [1] TRUE
```

`[[` will also collect.

```R
xrdd[[1]]
# [1]  1  2  3  4  5  6  7  8  9 10
```

`lapply_rdd` and `mapply_rdd` work similarly to their counterparts in base R.

```R
first3 <- lapply_rdd(xrdd, function(x) x[1:3])
collect(first3)
# [[1]]
# [1] 1 2 3
# 
# [[2]]
# [1] "a" "b" "c"
# 
# [[3]]
# [1] 0.3102394 1.5215550 0.9653850


yrdd <- rddlist(sc, list(21:30, LETTERS, rnorm(10)))
xyrdd <- mapply_rdd(c, xrdd, yrdd)
collect(xyrdd)
#[[1]]
# [1]  1  2  3  4  5  6  7  8  9 10 21 22 23 24 25 26 27 28 29 30
#
#[[2]]
# [1] "a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p" "q"
#"r" "s"
#[20] "t" "u" "v" "w" "x" "y" "z" "A" "B" "C" "D" "E" "F" "G" "H" "I" "J"
#"K" "L"
#[39] "M" "N" "O" "P" "Q" "R" "S" "T" "U" "V" "W" "X" "Y" "Z"
#
#[[3]]
# [1]  0.31023939  1.52155502  0.96538505  0.03847928  0.50945686
#1.72843202
# [7]  2.90275046  1.17984260 -0.11084538  0.49662442  0.73272756
#-0.04654643
#[13] -0.11754979 -1.19877078  0.94597951 -0.62537377  1.43749694
#-1.01762042
#[19] -0.20411707 -0.29942649
```
