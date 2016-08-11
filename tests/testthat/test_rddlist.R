# Basic tests for rddlist
library(sparkapi)

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
