# AllAboutWine
repo for wine price prediction

I have experimented into 2 predictors, simple linear regression and random forest regression, which all writen in Models.scala

This repo is written in scala. To run it, please follow the code below:

spark-shell --deploy-mode client -i  DataClean.scala
:q #quit the spark shell 
hdfs dfs -ls
#After running the cleaning code, there should be a file called CleanedTable.csv

spark-shell --deploy-mode client -i  PriceAnalysis.scala
:q #quit the spark shell 
hdfs dfs -ls
#After running the cleaning code, there should be a file called ProcessedWines.csv

spark-shell --deploy-mode client -i  Model.scala
