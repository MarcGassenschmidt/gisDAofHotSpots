package scenarios

import clustering.ClusterHotSpots
import export.SoHResult
import geotrellis.raster.Tile
import getisOrd.SoH.SoHR
import getisOrd.{SoH, Weight}
import parmeters.{Scenario, Settings}

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 28.06.17.
  */
class SpaceTimeScenario extends GenericScenario{


  def forGStar(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int) :Unit= forGStar(globalSettings, outPutResults, runs, false)
  def forFocalGStar(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int) :Unit= forGStar(globalSettings, outPutResults, runs, true)

  override def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.scenario = Scenario.Time.toString
    globalSettings.fromFile = false
    globalSettings.weightMatrix = Weight.Square
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10
    globalSettings.focalRange = 60 //TODO
    globalSettings.weightRadius = 30 //TODO
    forGStar(globalSettings, outPutResults, runs)
    forFocalGStar(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }

  def forGStar(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int, focal : Boolean): Unit = {
    for(h <- 0 to 23) {
      globalSettings.hour = h
        var totalTime = System.currentTimeMillis()
        globalSettings.focal = focal
        val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: SoHR, lat: (Int, Int)) = oneCase(globalSettings, h, runs)
        saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)

    }
  }

  override def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), SoHR, (Int, Int)) = {
    globalSettings.weightRadius = 20 //TODO
    val zoomStep = 1
    val tempH = globalSettings.hour
    globalSettings.hour = 0
    val mulitBandTile = getRasterFromMulitGeoTiff(globalSettings, zoomStep, runs, 0, "raster", getMulitRasterWithCorrectResolution(globalSettings, zoomStep, runs, 0)._1)
    globalSettings.hour = tempH
    val  raster = mulitBandTile.band(i)

    val gStarParent = getRasterFromGeoTiff(globalSettings, "gStar", gStar(raster, globalSettings, true))
    globalSettings.weightRadius = 30 //TODO
    val gStarChild = getRasterFromGeoTiff(globalSettings, "gStar", gStar(raster, globalSettings, true))

    println("G* End")

    val clusterParent = getRasterFromGeoTiff(globalSettings, "cluster",((new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))._1)
    val clusterChild =getRasterFromGeoTiff(globalSettings, "cluster", (new ClusterHotSpots(gStarChild)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1)
    val time = System.currentTimeMillis()
    val numberclusterParent = clusterParent.findMinMax._2
    val numberclusterChild = clusterChild.findMinMax._2
    System.out.println("Time for Number of Cluster:"+(System.currentTimeMillis()-time)/1000)
    println("End Cluster")
    visulizeCluster(globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), i==0)
    println("End Visual Cluster")

    val sohVal = SoH.getSoHDowAndUp(clusterParent,clusterChild)
    (globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), sohVal,
      ((10.0 + 990.0 / runs.toDouble * i).ceil.toInt, //Just lat for export
        (10.0 + 990.0 / runs.toDouble * i +1).ceil.toInt)) //Just lat for export
  }

  override def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    //TODO
  }

  override def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    //TODO
  }
}
