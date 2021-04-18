package com.minsait.ttaa.exercises

import org.apache.spark.sql.DataFrame
/** A FileReader that call a strategy to read a file base in the file suffix
 *
 *  @constructor create a new FileReader with a strategy
 *  @param strategy the name of strategy to follow when read a file
 */
class FileReader(strategy: (String) => Option[DataFrame]) {
  /**
   * Call the strategy to read a file with SparkSession
   * this return a Option[DataFrame]
   *
   * @param filePath is the file path where the file is storage
   */
  def read(filePath: String): Option[DataFrame] = {
    strategy(filePath)
  }
}
