package scenarios

import clustering.ClusterHotSpots
import export.SoHResult
import geotrellis.raster.Tile
import getisOrd.{SoH, Weight}
import parmeters.Settings

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 24.05.17.
  */
class DifferentRatio extends GenericScenario{

  override def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.weightMatrix = Weight.Square
    globalSettings.weightRadius = 2
    globalSettings.scenario = "Ratio"
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10

    forFocalG(globalSettings, outPutResults, runs)
    //forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }

  override def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 0 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = true
      if(i==0){
        globalSettings.fromFile = false
      } else {

        globalSettings.fromFile = false
      }

        //globalSettings.weightRadius = weightRatio(globalSettings, runs, j)
        val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double,Double,Double), lat : (Int,Int)) = oneCase(globalSettings, i, runs)
        saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)

    }
  }

  def weightRatio(globalSettings: Settings, runs: Int, j: Int): Double = {
    (globalSettings.focalRange / runs.toDouble) * j
  }



  override def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), (Double, Double, Double, Double), (Int, Int)) = {
    globalSettings.sizeOfRasterLat = 200
    globalSettings.sizeOfRasterLon = 200
    val raster : Tile = getRasterFromGeoTiff(globalSettings, 3, runs, 0, "raster", getRaster(globalSettings))


    globalSettings.focalRange =3+i*6
    val gStarParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "gStar", gStar(raster, globalSettings, true))
    globalSettings.focalRange = 3+(i+1)*6
    val gStarChild = getRasterFromGeoTiff(globalSettings, i, runs, 1, "gStar", gStar(raster, globalSettings, true))
    println("G* End")


    val clusterParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "cluster",((new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))._1)
    val clusterChild =getRasterFromGeoTiff(globalSettings, i, runs, 1, "cluster", (new ClusterHotSpots(gStarChild)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1)
    val time = System.currentTimeMillis()
    val numberclusterParent = clusterParent.findMinMax._2
    val numberclusterChild = clusterChild.findMinMax._2
    System.out.println("Time for Number of Cluster:"+(System.currentTimeMillis()-time)/1000)
    println("End Cluster")


    globalSettings.parent = true
    globalSettings.focalRange = 3+i*6
    visulizeCluster(globalSettings, clusterParent)
    globalSettings.parent = false
    globalSettings.focalRange = 3+(i+1)*6
    visulizeCluster(globalSettings, clusterChild)
    println("End Visual Cluster")


    val soh = new SoH()
    val sohVal :(Double,Double,Double,Double) = soh.getSoHDowAndUp((clusterParent,numberclusterParent),(clusterChild,numberclusterChild))
    (globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), sohVal,
      (3+i*6, //Just lat for export
      3+(i+1)*6)) //Just lat for export
  }




}
