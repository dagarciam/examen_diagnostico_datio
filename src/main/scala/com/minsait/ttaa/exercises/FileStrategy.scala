package com.minsait.ttaa.exercises

import org.apache.spark.sql.DataFrame
/** A FileStrategy is an Object that have the logic to read a file
 *
 */
object FileStrategy {

  def apply(filePath: String): (String) => Option[DataFrame] =
    filePath match {
      case file if file.endsWith(".csv") => csvToDataFrame
      case file => throw new RuntimeException(s"Unknown format: $file")
    }

  /**
   * csvToDataFrame read a csv file with headers and return a Option[DataFrame] using SparkSession
   *
   * @param file is the file path where the file is storage
   *
   * @return Option[DataFrame] is the result of read a CSV with SparkSession
   */
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
