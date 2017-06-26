package demo

import importExport.DownloadFilesFromWeb
import parmeters.Settings
import scenarios.{DifferentFocal, DifferentRasterSizes, DifferentRatio, GenericScenario}

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {
    println(System.getProperty("sun.arch.data.model"))
//    val downloader = new DownloadFilesFromWeb()
//    val setting = new Settings()
//    setting.csvMonth = 5
//    setting.csvYear = 2015
//    downloader.downloadNewYorkTaxiFiles(setting)

    (new DifferentRasterSizes).runScenario()

//    var scenario : GenericScenario = new DifferentRatio()
//    scenario.runScenario()
//    scenario = new DifferentRasterSizes()
//    scenario.runScenario()
//    scenario = new DifferentFocal()
//    scenario.runScenario()
  }

}
