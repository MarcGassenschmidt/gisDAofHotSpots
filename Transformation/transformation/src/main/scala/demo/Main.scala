package demo

import importExport.DownloadFilesFromWeb
import parmeters.Settings
import scenarios._

object Main {

  def main(args: Array[String]): Unit = {

    val downloader = new DownloadFilesFromWeb()
    val setting = new Settings()
    setting.csvMonth = 1
    setting.csvYear = 2016
    downloader.downloadNewYorkTaxiFiles(setting)

    (new SpaceTimeScenario).runScenario()

    var scenario : GenericScenario = new DifferentRatio()
    scenario.runScenario()
    scenario = new DifferentRasterSizes()
    scenario.runScenario()
    scenario   = new DifferentFocal()
    scenario.runScenario()
  }

}
