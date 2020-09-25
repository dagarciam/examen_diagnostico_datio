package com.minsait.ttaa

import com.minsait.ttaa.constants.StaticVals._

trait RunnerTrait {

  def main(args: Array[String]): Unit = {
    println(MESSAGE)
  }

}

object Runner extends RunnerTrait