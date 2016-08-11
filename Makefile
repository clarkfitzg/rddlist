all:
	R -q -e "roxygen2::roxygenize(clean=TRUE)"
	R CMD INSTALL .

clean:
	rm log4j.spark.log*
