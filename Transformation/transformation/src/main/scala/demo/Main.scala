package demo

import scenarios.{DifferentRasterSizes, DifferentRatio}

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {
    val scenario = new DifferentRasterSizes()
    //val scenario = new DifferentRatio()
    scenario.runScenario()
  }

}
