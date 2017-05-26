package scenarios

import clustering.ClusterHotSpots
import export.SoHResult
import geotrellis.raster.Tile
import getisOrd.SoH
import parmeters.Settings

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 24.05.17.
  */
class DifferentRatio extends GenericScenario{

  override def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.scenario = "Ratio"
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10

    forFocalG(globalSettings, outPutResults, runs)
    //forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }

  override def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 3 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = true
      if(i==0){
        globalSettings.fromFile = false
      } else {
        globalSettings.fromFile = false
      }
      for(j <-0 to runs){
        globalSettings.weightRadius = 3
        globalSettings.focalRange = 2+i*6
        //globalSettings.weightRadius = weightRatio(globalSettings, runs, j)
        val (para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)) = oneCase(globalSettings, i, runs)
        saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, paraChild, chs, sohVal)
      }
    }
  }

  def weightRatio(globalSettings: Settings, runs: Int, j: Int): Double = {
    (globalSettings.focalRange / runs.toDouble) * j
  }



  override def getParentChildSetting(global : Settings): (Settings, Settings) = {
    val para = new Settings()
    para.scenario = global.scenario
    para.weightRadius = global.weightRadius
    para.focalRange = global.focalRange
    para.sizeOfRasterLat = global.sizeOfRasterLat
    para.sizeOfRasterLon = global.sizeOfRasterLon
    para.focal = global.focal
    val paraChild = new Settings()
    paraChild.scenario = global.scenario
    paraChild.focal = global.focal
    paraChild.focalRange = global.focalRange
    paraChild.sizeOfRasterLat = global.sizeOfRasterLat
    paraChild.sizeOfRasterLon = global.sizeOfRasterLon
    paraChild.parent = false
    paraChild.weightRadius = global.weightRadius-1
    (para, paraChild)
  }



}
