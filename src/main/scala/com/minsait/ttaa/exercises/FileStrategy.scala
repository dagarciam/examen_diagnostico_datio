package com.minsait.ttaa.exercises

import org.apache.spark.sql.DataFrame

object FileStrategy {

  def apply(filePath: String): (String) => Option[DataFrame] =
    filePath match {
      case file if file.endsWith(".csv") => csvToDataFrame
      case file => throw new RuntimeException(s"Unknown format: $file")
    }

  def csvToDataFrame(file: String): Option[DataFrame] = {
    try{
      Some(SparkCommon
        .getSparkSession()
        .read
        .option("header", true)
        .csv(file))
    }catch {
      case e:Exception => None
    }

  }

}
