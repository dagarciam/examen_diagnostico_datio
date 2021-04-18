package com.minsait.ttaa.exercises

import org.apache.spark.sql.SparkSession

object SparkCommon {

  private val master = "local[*]"
  private val appName = "Exercises"

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()
  }

  def stopSparkSession(): Unit = {
    getSparkSession().stop()
  }

}
