package scenarios

import java.io.{File, FileOutputStream, PrintWriter}

import au.com.bytecode.opencsv.CSVWriter
import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd._
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 19.05.17.
  */
class DifferentRasterSizes extends GenericScenario{



  override def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.weightMatrix = Weight.Square
    globalSettings.weightRadius = 2
    globalSettings.scenario = "RatioZoom"
    var outPutResults = ListBuffer[SoHResult]()
    val runs = 5

//        forGlobalG(globalSettings, outPutResults, runs)
//        saveResult(globalSettings, outPutResults)
    outPutResults = ListBuffer[SoHResult]()
    forFocalG(globalSettings, outPutResults, runs)

    saveResult(globalSettings, outPutResults)
  }

  override def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for(k <- 0 to 9) {
      globalSettings.zoomLevel = k
      //globalSettings.weightRadius = 1+k*2
      for (i <- 1 to 5) {
        var totalTime = System.currentTimeMillis()
        globalSettings.focal = false
        globalSettings.focalRange = 0
        if (k == 0) {
          globalSettings.fromFile = false
        } else {
          globalSettings.fromFile = false
        }

        val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double), lat: (Int, Int)) = oneCase(globalSettings, i, runs)
        saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)


      }
    }
  }

  override def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for(k <- 0 to 9){
      //globalSettings.zoomLevel = k
      globalSettings.weightRadius = 1+k*2
      for (j <- 0 to 9) {
        //globalSettings.weightRadius =3+j*2
        globalSettings.focalRange = 2+j*6
        for (i <- 1 to 5) {
          logger.info("k,j,i:"+k+","+j+","+i)
          val totalTime = System.currentTimeMillis()
          globalSettings.focal = true
          if (k == 0) {
            globalSettings.fromFile = false
          } else {
            globalSettings.fromFile = false
          }

          if(globalSettings.weightRadius<globalSettings.focalRange){
            //globalSettings.weightRadius = weightRatio(globalSettings, runs, j)
            val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double), lat: (Int, Int)) = oneCase(globalSettings, i, runs)
            saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
          }
        }
      }
    }

  }

  def weightRatio(globalSettings: Settings, runs: Int, j: Int): Double = {
    (globalSettings.focalRange / runs.toDouble) * j
  }



  override def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), (Double, Double), (Int, Int)) = {
    globalSettings.sizeOfRasterLat = 100
    globalSettings.sizeOfRasterLon = 100

    globalSettings.zoomLevel = i
    val raster : Tile = getRasterFromGeoTiff(globalSettings, 3, runs, 0, "raster", getRaster(globalSettings))
    globalSettings.zoomLevel = i+1
    val rasterParent : Tile = getRasterFromGeoTiff(globalSettings, 3, runs, 0, "raster", getRaster(globalSettings))


    //globalSettings.focalRange = 2+i*6
    //globalSettings.weightRadius = 1+i*2
    val gStarParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "gStar", gStar(raster, globalSettings, true))
    //globalSettings.focalRange = 2+(i+1)*6
    //globalSettings.weightRadius = 1+(i+1)*2
    val gStarChild = getRasterFromGeoTiff(globalSettings, i, runs, 1, "gStar", gStar(rasterParent, globalSettings, true))

    //globalSettings.weightRadius = 1+i*2
    println("G* End")


    val clusterParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "cluster",((new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))._1)
    val clusterChild =getRasterFromGeoTiff(globalSettings, i, runs, 1, "cluster", (new ClusterHotSpots(gStarChild)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1)
    val time = System.currentTimeMillis()
    val numberclusterParent = clusterParent.findMinMax._2
    val numberclusterChild = clusterChild.findMinMax._2
    System.out.println("Time for Number of Cluster:"+(System.currentTimeMillis()-time)/1000)
    println("End Cluster")


    globalSettings.parent = true
    //globalSettings.focalRange = 3+i*6
    visulizeCluster(globalSettings, clusterParent)
    globalSettings.parent = false
    //globalSettings.focalRange = 3+(i+1)*6
    //visulizeCluster(globalSettings, clusterChild)
    println("End Visual Cluster")


    val soh = new SoH()
    val sohVal :(Double,Double) = soh.getSoHDowAndUp((clusterParent),(clusterChild))
    (globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), sohVal,
      (3+i*6, //Just lat for export
        3+(i+1)*6)) //Just lat for export
  }












}
