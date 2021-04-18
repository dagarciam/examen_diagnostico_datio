package com.minsait.ttaa

import com.minsait.ttaa.exercises.{Exercises, SparkCommon}

trait RunnerTrait {

  def main(args: Array[String]): Unit = {
    val exercises = new Exercises(SparkCommon.getSparkSession())
    exercises.exercise01()
    exercises.exercise02()
    exercises.exercise03()
  }

}

object Runner extends RunnerTrait