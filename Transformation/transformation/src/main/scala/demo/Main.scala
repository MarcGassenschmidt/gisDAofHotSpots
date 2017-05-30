package demo

import `import`.DownloadFilesFromWeb
import parmeters.Settings
import scenarios.{DifferentRasterSizes, DifferentRatio}

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {
    println(System.getProperty("sun.arch.data.model"))
//    val downloader = new DownloadFilesFromWeb()
//    val setting = new Settings()
//    setting.csvMonth = 5
//    setting.csvYear = 2015
//    downloader.downloadNewYorkTaxiFiles(setting)
    val scenario = new DifferentRasterSizes()
    //val scenario = new DifferentRatio()
    scenario.runScenario()
  }

}
