package com.minsait.ttaa.exercises

import org.apache.spark.sql.DataFrame

class FileReader(strategy: (String) => Option[DataFrame]) {
  def read(filePath: String): Option[DataFrame] = {
    strategy(filePath)
  }
}
