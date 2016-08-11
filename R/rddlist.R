# sparkapi provides all the invoke functions
#' @importFrom sparkapi invoke invoke_new invoke_static


# A basic R list implemented in Spark.
# 
# Each element of the local R list corresponds to an element of the Spark RDD.
rddlist = function(sc, data, cache=TRUE){

    if(any(class(data) == "rddlist")){
        return(data)
    }

    if(!is.list(data)){
        stop("data should be a list")
    }

    serial_parts = lapply(data, serialize, connection = NULL)

    # An RDD of the serialized R parts
    # This is class org.apache.spark.api.java.JavaRDD
    RDD = invoke_static(sc,
                        "org.apache.spark.api.r.RRDD",
                        "createRDDFromArray",
                        sparkapi::java_context(sc),
                        serial_parts)

    # (data, integer) pairs
    backwards = invoke(RDD, "zipWithIndex")

    # An RDD of integers
    index = invoke(backwards, "values")

    # The pairRDD of (integer, data) 
    pairRDD = invoke(index, "zip", RDD)

    # This is all written specifically for bytes, so should be fine to let this 
    # classTag hang around
    new_rddlist(pairRDD, invoke(RDD, "classTag"), cache)
}


# Create an instance of rddlist as subclass of sparkapi spark_jobj
new_rddlist <- function(pairRDD, classTag, cache){
    out <- pairRDD
    attr(out, "classTag") <- classTag
    class(out) <- c("rddlist", class(pairRDD))
    if(cache) invoke(out, "cache")
    out
}


lapply_rdd <- function(X, FUN, cache=TRUE){
# TODO: support dots function(X, FUN, ...){

    # The function should be in a particular form for calling Spark's
    # org.apache.spark.api.r.RRDD class constructor
    FUN_applied = function(partIndex, part) {
        FUN(part)
    }
    FUN_clean = cleanClosure(FUN_applied)

    # TODO: Could come back and implement this functionality later
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()
    # I believe broadcastArr holds these broadcast variables:
    # https://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables
    # But what's the relation between broadcast variables, FUN's closure,
    # and the ... argument?
    
    vals = invoke(X, "values")

    # Use Spark to apply FUN
    fxrdd <- invoke_new(sparkapi::spark_connection(X),
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       invoke(vals, "rdd"),
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       X$classTag
                       )

    # Convert this into class org.apache.spark.api.java.JavaRDD so we can
    # zip
    JavaRDD = invoke(fxrdd, "asJavaRDD")

    # Reuse the old index to create the PairRDD
    index = invoke(X, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)
   
    new_rddlist(pairRDD, X$classTag, cache)
}


#`[[.rddlist` <- function(x, i, j){
`[[.rddlist` <- function(x, i){
    javaindex = as.integer(i - 1L)
    javabytes = invoke(x, "lookup", javaindex)
    # The bytes come wrapped in a list
    bytes = invoke(javabytes, "toArray")[[1]]
    unserialize(bytes)
}


# a_nested = TRUE means that a is already in the form of a nested list with
# two layers: [ [a1], [a2], ... , [an] ]
# 
# Would be better to have this function be private and zip_rdd be the
# public exported function.
#
zip2 = function(a, b, a_nested = FALSE, b_nested = FALSE){
    # They must be nested for this to work
    if(!a_nested){
        a = lapply_rdd(a, list)
    }
    if(!b_nested){
        b = lapply_rdd(b, list)
    }
    aval = invoke(a, "values")
    bval = invoke(b, "values")
    # class org.apache.spark.api.java.JavaPairRDD

    zipped = invoke(aval, "zip", bval)
    # class org.apache.spark.rdd.ZippedPartitionsRDD2
    # This has the same number of elements as the input a.
    # Converting to rdd seems necessary for the invoke_new below
    RDD = invoke(zipped, "rdd")

    partitionFunc <- function(partIndex, part) {
        part
    }
    FUN_clean = cleanClosure(partitionFunc)
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()

    pairs <- invoke_new(sparkapi::spark_connection(a),
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       RDD,
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       a$classTag
                       )

    JavaRDD = invoke(pairs, "asJavaRDD")
    index = invoke(a, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)

    new_rddlist(pairRDD, a$classTag, cache=FALSE)
}


# For rdds a, b, ... of the same length n this creates an rddlist of length n
# where the ith element has the value list(a[[i]], b[[i]], ... )
#
# It always returns a nested list.
#
zip_rdd = function(..., cache=TRUE){
    args = list(...)
    a = args[[1]]
    n = length(args)

    zipped = lapply_rdd(a, list, cache = FALSE)

    if(n == 1L){
        # Easy out for trivial case
        # Note zipping will always have the same nested structure
        return(zipped)
    }

    # A 'reduce' operation
    for(i in 2:n){
        zipped = zip2(zipped, args[[i]], a_nested = TRUE)
    }

    # The idea with caching is to avoid it in the intermediate steps
    # TODO: verify that this strategy is actually useful
    if(cache) invoke(zipped, "cache")

    zipped
}


# A version of mapply that works with rddlists
# ... should be rddlists
mapply_rdd = function(FUN, ..., cache = TRUE){

    # TODO: add recycling, Moreargs

    FUN = match.fun(FUN)
    zipped = zip_rdd(..., cache = FALSE)
    
    # The parts in zipped are always lists
    zipFUN = function(zipped_part){
        do.call(FUN, zipped_part)
    }

    lapply_rdd(zipped, zipFUN, cache)
}


length_rdd = function(rdd){
    # sparkapi maps Java long -> double
    as.integer(invoke(rdd, "count"))
}


# Collects and unserializes the entire rdd from Spark back into local R.
collect = function(rdd){
    values = invoke(rdd, "values")
    collected = invoke(values, "collect")
    rawlist = invoke(collected, "toArray")
    lapply(rawlist, unserialize)
}


if(TRUE){
# Basic tests for rddlist

library(sparkapi)
library(testthat)

# This gets us cleanClosure
source("utils.R")

if(!exists("sc")){
    sc <- start_shell(master = "local")
}

x = list(1:10, letters, rnorm(10))
xrdd = rddlist(sc, x)

############################################################

test_that("round trip serialization", {

    collected = collect(xrdd)
    expect_equal(x, collected)

})

test_that("simple indexing", {
    i = 1
    expect_equal(x[[i]], xrdd[[i]])
})

test_that("lapply_rdd", {

    first5 = function(x) x[1:5]
    fx = lapply(x, first5)
    fxrdd = lapply_rdd(xrdd, first5)

    fxrdd_collected = collect(fxrdd)

    expect_equal(fx, fxrdd_collected)
})

test_that("zipping several RDD's", {

    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))

    ar = rddlist(sc, a)
    br = rddlist(sc, b)

    abzip = Map(list, a, b)
    abzip_rdd = zip2(ar, br)

    abzip_rdd_collected = collect(abzip_rdd)

    expect_equal(abzip, abzip_rdd_collected)

    expect_equal(abzip, collect(zip_rdd(ar, br)))

    # Now for 3+
    c = list(101:110, rnorm(5), rnorm(7))
    cr = rddlist(sc, c)

    abczip = Map(list, a, b, c, a)
    abczip_rdd = zip_rdd(ar, br, cr, ar)

    abczip_rdd_collected = collect(abczip_rdd)

    expect_equal(abczip, abczip_rdd_collected)

})

test_that("mapply", {

    y = list(21:30, LETTERS, rnorm(10))
    yrdd = rddlist(sc, y)
   
    xy = mapply(c, x, y)

    xyrdd = collect(mapply_rdd(c, xrdd, yrdd))

    expect_equal(xy, xyrdd)
})

test_that("length", {

    expect_identical(length(x), length_rdd(xrdd),
        info = "This needs to be an integer")

})

}
