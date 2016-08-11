# Anything with invoke comes from sparkapi
#' @importFrom sparkapi invoke invoke_new invoke_static
NULL

#' Create an rddlist from a local R list.
#' 
#' Each element of the local R list corresponds to an element of the Spark
#' RDD (Resilient Distributed Dataset)
#'
#' @param sc Spark connection as returned from
#'      \code{\link[sparkapi]{start_shell}}
#' @param X local R list.
#' @param cache logical - Should the resulting RDD be cached in Spark's
#'      memory?
#'
#' @return rddlist A Spark Java Object representing the rddlist
#'
#' @examples
#' x <- list(1:10, letters, rnorm(10))
#' xrdd <- rddlist(sc, x)
#'
#' @export
rddlist <- function(sc, X, cache=TRUE){
    if(!is.list(X)){
        stop("X should be a list")
    }

    serial_parts = lapply(X, serialize, connection = NULL)

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


#' new rddlist
#'
#' Create an instance of rddlist as subclass of sparkapi spark_jobj
#'
new_rddlist <- function(pairRDD, classTag, cache){
    out <- pairRDD
    attr(out, "classTag") <- classTag
    class(out) <- c("rddlist", class(pairRDD))
    if(cache) invoke(out, "cache")
    out
}


#' Apply a Function over an rddlist
#' 
#' Modeled after \code{lapply} in base R
#'
#' @param X rddlist
#' @param FUN function to apply to each element of X
#' @param cache logical - Should the resulting RDD be cached in Spark's
#'      memory?
#'
#' @return rddlist A Spark Java Object representing the resulting rddlist
#'
#' @examples
#' x <- list(1:10, letters, rnorm(10))
#' xrdd <- rddlist(sc, x)
#' lapply_rdd(xrdd, head)
#'
#' @export
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


#' Collect the ith element of an rddlist
#' @export
`[[.rddlist` <- function(x, i){
    javaindex = as.integer(i - 1L)
    javabytes = invoke(x, "lookup", javaindex)
    # The bytes come wrapped in a list
    bytes = invoke(javabytes, "toArray")[[1]]
    unserialize(bytes)
}


#' zip 2 rddlists into one
#'
#' This does the actual work for zip_rdd
#'
#' a_nested = TRUE means that a is already in the form of a nested list with
#' two layers: [ [a1], [a2], ... , [an] ]
zip2 <- function(a, b, a_nested = FALSE, b_nested = FALSE){
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


#' Zip rdds together
#' 
#' For rdds a, b, ... of the same length n this creates an rddlist of length n
#' where the ith element has the value list(a[[i]], b[[i]], ... )
#'
#' It always returns a nested list.
#'
#' @param ... rddlists of the same length
#' @param cache logical - Should the resulting RDD be cached in Spark's
#'      memory?
#'
#' @return rddlist A Spark Java Object representing the resulting rddlist
zip_rdd <- function(..., cache=TRUE){
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


#' Apply a Function to multiple rddlist arguments
#' 
#' Modeled after \code{mapply} in base R
#'
#' @param FUN function to apply to each element of X
#' @param ... rddlists of the same length
#' @param cache logical - Should the resulting RDD be cached in Spark's
#'      memory?
#'
#' @return rddlist A Spark Java Object representing the resulting rddlist
#'
#' @examples
#' x <- rddlist(sc, list(1:10, letters, rnorm(10)))
#' y <- rddlist(sc, list(21:30, LETTERS, rnorm(10)))
#' xy <- mapply_rdd(c, x, y)
#'
#' @export
mapply_rdd <- function(FUN, ..., cache = TRUE){

    # TODO: add recycling, Moreargs
    FUN = match.fun(FUN)
    zipped = zip_rdd(..., cache = FALSE)
    
    # The parts in zipped are always lists
    zipFUN = function(zipped_part){
        do.call(FUN, zipped_part)
    }

    lapply_rdd(zipped, zipFUN, cache)
}


#' length of rddlist
#' 
#' @param rdd rddlist
#'
#' @return n integer length 
#' @export
length_rdd <- function(rdd){
    # sparkapi maps Java long -> double
    as.integer(invoke(rdd, "count"))
}


#' Collect from Spark
#' 
#' Collects and unserializes the entire rdd from Spark back into local R.
#' 
#' @param rdd rddlist
#'
#' @return list The contents of rddlist in a local R list
#'
#' @examples
#' x <- list(1:10, letters, rnorm(10))
#' xrdd <- rddlist(sc, x)
#' x2 <- collect(xrdd)
#' identical(x, x2)
#' @export
collect <- function(rdd){
    values = invoke(rdd, "values")
    collected = invoke(values, "collect")
    rawlist = invoke(collected, "toArray")
    lapply(rawlist, unserialize)
}
