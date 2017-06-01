package scenarios

import java.io.{File, PrintWriter}

import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd.{GetisOrd, GetisOrdFocal, SoH, Weight}
import org.joda.time.DateTime
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 24.05.17.
  */
class GenericScenario {

  def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.weightMatrix = Weight.Square
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10

    forFocalG(globalSettings, outPutResults, runs)
    //forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }



  def saveResult(settings: Settings, outPutResults: ListBuffer[SoHResult]): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    val dir = settings.ouptDirectory+settings.scenario+"/"
    val f = new File(dir)
    f.mkdirs()
    val pw = new PrintWriter(new File(dir+DateTime.now().toString("dd_MM___HH_mm_")+"result.csv"))
    outPutResultPrinter.printResults(outPutResults,false,pw)
    //outPutResults.map(x => pw.println(x.format()))
    pw.flush()
    pw.close()
    val pwShort = new PrintWriter(new File(dir+DateTime.now().toString("dd_MM___HH_mm_")+"short_result.csv"))
    outPutResultPrinter.printResults(outPutResults,true, pwShort)
    pwShort.flush()
    pwShort.close()
  }

  def saveSoHResults(totalTime: Long, outPutResults: ListBuffer[SoHResult], globalSettings: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double, Double, Double), lat : (Int,Int)): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      globalSettings,
      ((System.currentTimeMillis() - totalTime) / 1000),
      sohVal,
      lat._1)
    println(outPutResultPrinter.printResults(outPutResults,true))
  }

  def visulizeCluster(setting: Settings, chs: ((Tile, Int), (Tile, Int)), first : Boolean): Unit = {
    val image = new TileVisualizer()
    if(first){
      image.visualTileNew(chs._1._1, setting, "cluster")
    }
    image.visualTileNew(chs._2._1, setting, "cluster")
  }

  def gStar(tile : Tile, tile_plusOne : Tile, settings : parmeters.Settings, first : Boolean): (Tile, Tile) = {
    var startTime = System.currentTimeMillis()
    var ord : GetisOrd = null
    var ord_plusOne : GetisOrd = null
    if(settings.focal){
      ord = new GetisOrdFocal(tile, settings)
      ord_plusOne = new GetisOrdFocal(tile_plusOne, settings)
    } else {
      ord = new GetisOrd(tile, settings)
      ord_plusOne = new GetisOrd(tile_plusOne, settings)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    val score =(ord.gStarComplete(),ord_plusOne.gStarComplete())
    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    val image = new TileVisualizer()
    startTime = System.currentTimeMillis()
    if(first){
      image.visualTileNew(score._1, settings, "gStar")
    }
    image.visualTileNew(score._2, settings, "gStar")
    println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    score
  }


  def getRaster(settings : Settings): Tile = {
    val serilizer = new SerializeTile(settings.serilizeDirectory)
    if(settings.fromFile){
      val raster = creatRaster(settings)
      serilizer.write(raster)
      return raster
    } else {
      return serilizer.read()
    }
  }

  def creatRaster(settings : Settings): Tile = {
    var startTime = System.currentTimeMillis()
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    //arrayTile.histogram.values().map(x => println(x))
    println("Time for RasterTransformation =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + arrayTile.cols + "," + arrayTile.rows + ")")
    arrayTile
  }

  def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 1 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = false
      if(i==0){
        globalSettings.fromFile = false
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double,Double,Double),  lat : (Int,Int)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
    }
  }

  def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 1 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = true
      globalSettings.focalRange = 30
      if(i==0){
        globalSettings.fromFile = false
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double, Double, Double), lat : (Int,Int)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
    }
  }

  def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), (Double, Double, Double, Double), (Int, Int)) = {
    val raster_plus1 = getRasterWithCorrectResolution(globalSettings, i, runs, 1)
    val raster = getRasterWithCorrectResolution(globalSettings, i, runs, 0)

    //val image = new TileVisualizer()
    //image.visualTileNew(raster, globalSettings, "plainRaster")

    val score = gStar(raster._1, raster_plus1._1, globalSettings, i==0)
    println("G* End")
    val chs = ((new ClusterHotSpots(score._1)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue),
      (new ClusterHotSpots(score._2)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))
    println("End Cluster")
    visulizeCluster(globalSettings, chs, i==0)
    println("End Visual Cluster")
    val soh = new SoH()
    val sohVal :(Double,Double,Double,Double) = soh.getSoHDowAndUp(chs)
    (globalSettings, chs, sohVal, (raster._2, raster_plus1._2))
  }

  def getRasterWithCorrectResolution(globalSettings: Settings, i : Int, runs : Int, next : Int): (Tile,Int,Int) = {
    val actualLat = ((globalSettings.latMax - globalSettings.latMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    val actualLon = ((globalSettings.lonMax - globalSettings.lonMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt

    var raster_plus1 = getRaster(globalSettings)
    if (!globalSettings.fromFile) {
      raster_plus1 = raster_plus1.resample(actualLat, actualLon)
    }
    (raster_plus1,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt)
  }


}
