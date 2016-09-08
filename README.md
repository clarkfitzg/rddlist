rddlist
================

**ATTENTION: This package should be considered as an experimental beta version only. I have yet to test it on a real cluster :)**

Please see [SparkR](https://spark.apache.org/docs/latest/sparkr.html) for the official Apache supported Spark / R interface.

**`rddlist`** Implements distributed computation on an R list with an [Apache Spark](http://spark.apache.org/) backend. This allows an R programmer to use familiar operations like `[[`, `lapply`, and `mapply` from within R to perform computation on larger data sets using a Spark cluster. This is a powerful combination, as any data in R can be represented with a list, and `*apply` operations allow the application of arbitrary user defined functions, provided they are pure. So this should work well for embarrassingly parallel problems.

The main purpose of this project is to serve as an object that will connect Spark to the more general [ddR project](https://github.com/vertica/ddR) (distributed data in R). Work supported by [R-Consortium](https://www.r-consortium.org/projects).

Under the hood this works by serializing each element of the list into a byte array and storing it in a Spark Pair RDD (resilient distributed data set).

Examples
--------

sparklyr provides the spark connections.

``` r
library(sparklyr)
spark_install(version = "2.0.0")
```

``` r
library(sparklyr)
library(rddlist)

sc <- spark_connect(master = "local", version = "2.0.0")

x <- list(1:10, letters, rnorm(10))
xrdd <- rddlist(sc, x)
```

`xrdd` is an object in the local R session referencing the actual data residing in Spark. Collecting deserializes the object from Spark into local R.

``` r
x2 <- collect(xrdd)
identical(x, x2)
```

    ## [1] TRUE

There is an exact correspondence between the structures in Spark and local R to simplify reasoning about how the data is stored in Spark.

`[[` will also collect.

``` r
xrdd[[1]]
```

    ##  [1]  1  2  3  4  5  6  7  8  9 10

`lapply_rdd` and `mapply_rdd` work similarly to their counterparts in base R.

``` r
first3 <- lapply_rdd(xrdd, function(x) x[1:3])
collect(first3)
```

    ## [[1]]
    ## [1] 1 2 3
    ## 
    ## [[2]]
    ## [1] "a" "b" "c"
    ## 
    ## [[3]]
    ## [1] -0.06828355  1.71073618 -0.21328809

``` r
yrdd <- rddlist(sc, list(21:30, LETTERS, rnorm(10)))
xyrdd <- mapply_rdd(c, xrdd, yrdd)
collect(xyrdd)
```

    ## [[1]]
    ##  [1]  1  2  3  4  5  6  7  8  9 10 21 22 23 24 25 26 27 28 29 30
    ## 
    ## [[2]]
    ##  [1] "a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p" "q"
    ## [18] "r" "s" "t" "u" "v" "w" "x" "y" "z" "A" "B" "C" "D" "E" "F" "G" "H"
    ## [35] "I" "J" "K" "L" "M" "N" "O" "P" "Q" "R" "S" "T" "U" "V" "W" "X" "Y"
    ## [52] "Z"
    ## 
    ## [[3]]
    ##  [1] -0.06828355  1.71073618 -0.21328809  0.17049709  1.31152251
    ##  [6]  0.02082922  1.27179006 -0.12391336  1.03659403 -1.04706867
    ## [11] -0.75988449  1.35119966 -0.61826324 -0.09526833  1.41975411
    ## [16] -1.36354139 -1.39366419 -0.11580074 -0.39300783 -0.86039328

``` r
spark_disconnect(sc)
```
