all:
	R -q -e "roxygen2::roxygenize(clean=TRUE)"
	R CMD INSTALL .

check:
	R CMD CHECK .

test:
	R -q -e "devtools::test()"

clean:
	rm tests/testthat/log4j.spark.log*
	rm -r ..Rcheck
	rm man/*
