package scripts

import importExport.PathFormatter
import parmeters.{Scenario, Settings}

/**
  * Created by marc on 04.09.17.
  */
object ResultScript {

  def main(args: Array[String]): Unit = {
    val settings = MetrikValidation.getScenarioSettings()

    for(s <- settings){
      var tmp = PathFormatter.getAllResultsFor(s)
      val median = tmp.getMedian()
      val time = tmp.getTime()
      val vali = tmp.getValidation()
      println("next")
    }
  }


}
