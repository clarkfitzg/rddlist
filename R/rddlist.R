require("methods")
# sparkapi provides all the invoke functions
#' @importFrom sparkapi invoke invoke_new invoke_static

# TODO: Would be better to turn this on as an option in Spark
# Not using caching currently while developing
CACHE_DEFAULT = TRUE

# Use S4 for consistency with ddR
setOldClass("spark_jobj")
setOldClass("spark_connection")
setClass("rddlist", slots = list(sc = "spark_connection",
                                 pairRDD = "spark_jobj",
                                 nparts = "integer",
                                 classTag = "spark_jobj"))


# A basic R list implemented in Spark.
# 
# Each element of the list local R list corresponds to an element of the Spark RDD.
rddlist = function(sc, data){

    if(class(data) == "rddlist"){
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

    # This is all written specifically for bytes, so should be fine to let this 
    # classTag have a slot.
    classTag = invoke(RDD, "classTag")  # Array[byte]

    # (data, integer) pairs
    backwards = invoke(RDD, "zipWithIndex")

    # An RDD of integers
    index = invoke(backwards, "values")

    # The pairRDD of (integer, data) 
    pairRDD = invoke(index, "zip", RDD)

    new("rddlist", sc = sc, pairRDD = pairRDD, nparts = length(data),
        classTag = classTag)
}


setMethod("lapply", signature(X = "rddlist", FUN = "function"),
function(X, FUN){
# TODO: support dots
# function(X, FUN, ...){

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
    
    vals = invoke(X@pairRDD, "values")

    # Use Spark to apply FUN
    fxrdd <- invoke_new(X@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       invoke(vals, "rdd"),
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       X@classTag
                       )

    # Convert this into class org.apache.spark.api.java.JavaRDD so we can
    # zip
    JavaRDD = invoke(fxrdd, "asJavaRDD")

    # Reuse the old index to create the PairRDD
    index = invoke(X@pairRDD, "keys")
    pairRDD = invoke(index, "zip", JavaRDD)
   
    output = X
    output@pairRDD = pairRDD
    output
})


setMethod("[[", signature(x = "rddlist", i = "numeric", j = "missing"),
function(x, i, j){
    javaindex = as.integer(i - 1L)
    javabytes = invoke(x@pairRDD, "lookup", javaindex)
    # The bytes come wrapped in a list
    bytes = invoke(javabytes, "toArray")[[1]]
    unserialize(bytes)
})


# a_nested = TRUE means that a is already in the form of a nested list with
# two layers: [ [a1], [a2], ... , [an] ]
# 
# Would be better to have this function be private and zip_rdd be the
# public exported function.
#
zip2 = function(a, b, a_nested = FALSE, b_nested = FALSE){
    # They must be nested for this to work
    if(!a_nested){
        a = lapply(a, list)
    }
    if(!b_nested){
        b = lapply(b, list)
    }
    aval = invoke(a@pairRDD, "values")
    bval = invoke(b@pairRDD, "values")
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
    # Copying logic from lapply - come back and refactor
    packageNamesArr <- serialize(NULL, NULL)
    broadcastArr <- list()
    # Same length
    pairs <- invoke_new(a@sc,
                       "org.apache.spark.api.r.RRDD",  # A new instance of this class
                       RDD,
                       serialize(FUN_clean, NULL),
                       "byte",  # name of serializer / deserializer
                       "byte",  # name of serializer / deserializer
                       packageNamesArr,  
                       broadcastArr,
                       a@classTag
                       )
    JavaRDD = invoke(pairs, "asJavaRDD")
    # Reuse the old index to create the PairRDD
    index = invoke(a@pairRDD, "keys")
    # Same length
    pairRDD = invoke(index, "zip", JavaRDD)
    output = a
    output@pairRDD = pairRDD
    output
}


zip_rdd = function(...){
    args = list(...)
    a = args[[1]]
    n = length(args)

    zipped = lapply(a, list)

    if(n == 1L){
        # Easy out for trivial case
        # Note zipping will always have the same nested structure
        return(zipped)
    }

    # A 'reduce' operation
    for(i in 2:n){
        zipped = zip2(zipped, args[[i]], a_nested = TRUE)
    }
    zipped
}


# A version of mapply that works with rddlists
# ... should be rddlists
mapply_rdd = function(FUN, ...){

    # TODO: add recycling, Moreargs

    FUN = match.fun(FUN)
    zipped = zip_rdd(...)
    
    # The parts in zipped are always lists
    zipFUN = function(zipped_part){
        do.call(FUN, zipped_part)
    }

    lapply(zipped, zipFUN)
}


length_rdd = function(rdd){
    # sparkapi maps Java long -> double
    as.integer(invoke(rdd@pairRDD, "count"))
}


# Collects and unserializes from Spark back into local R.
collect_rdd = function(rdd){
    values = invoke(rdd@pairRDD, "values")
    collected = invoke(values, "collect")
    rawlist = invoke(collected, "toArray")
    lapply(rawlist, unserialize)
}


if(TRUE){
# Basic tests for rddlist

library(sparkapi)
library(testthat)

# This gets us cleanClosure
source('utils.R')

if(!exists('sc')){
    sc <- start_shell(master = "local")
}

x = list(1:10, letters, rnorm(10))
xrdd = rddlist(sc, x)

############################################################

test_that("round trip serialization", {

    collected = collect_rdd(xrdd)
    expect_equal(x, collected)

})

test_that("simple indexing", {
    i = 1
    expect_equal(x[[i]], xrdd[[i]])
})

test_that("zipping several RDD's", {

    set.seed(37)
    a = list(1:10, rnorm(5), rnorm(3))
    b = list(21:30, rnorm(5), rnorm(3))

    ar = rddlist(sc, a)
    br = rddlist(sc, b)

    abzip = Map(list, a, b)
    abzip_rdd = zip2(ar, br)

    abzip_rdd_collected = collect_rdd(abzip_rdd)

    expect_equal(abzip, abzip_rdd_collected)

    expect_equal(abzip, collect_rdd(zip_rdd(ar, br)))

    # Now for 3+
    c = list(101:110, rnorm(5), rnorm(7))
    cr = rddlist(sc, c)

    abczip = Map(list, a, b, c, a)
    abczip_rdd = zip_rdd(ar, br, cr, ar)

    abczip_rdd_collected = collect_rdd(abczip_rdd)

    expect_equal(abczip, abczip_rdd_collected)

})

test_that("lapply", {

    first5 = function(x) x[1:5]
    fx = lapply(x, first5)
    fxrdd = collect_rdd(lapply(xrdd, first5))

    expect_equal(fx, fxrdd)
})

test_that("mapply", {

    y = list(21:30, LETTERS, rnorm(10))
    yrdd = rddlist(sc, y)
   
    xy = mapply(c, x, y)

    xyrdd = collect_rdd(mapply_rdd(c, xrdd, yrdd))

    expect_equal(xy, xyrdd)
})

test_that("length", {

    expect_identical(length(x), length_rdd(xrdd),
        info = "This needs to be an integer")

})

}
