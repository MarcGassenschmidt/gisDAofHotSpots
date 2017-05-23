package scenarios

import java.io.{File, FileOutputStream, PrintWriter}

import au.com.bytecode.opencsv.CSVWriter
import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd.{GenericGetisOrd, GetisOrd, GetisOrdFocal, SoH}
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 19.05.17.
  */
class DifferentRasterSizes {

  def saveResult(settings: Settings, outPutResults: ListBuffer[SoHResult]): Unit = {
    val pw = new PrintWriter(new File(settings.ouptDirectory+"result.csv"))
    outPutResults.map(x => pw.println(x.format()))
    pw.flush()
    pw.close()
  }

  def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10

    forFocalG(globalSettings, outPutResults, runs)
    //forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
  }

  def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 0 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = true
      globalSettings.focalRange = 22
      if(i==0){
        globalSettings.fromFile = false
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, paraChild, chs, sohVal)
    }
  }

  def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 0 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = false
      if(i==0){
        globalSettings.fromFile = false
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, paraChild, chs, sohVal)
    }
  }

  def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, Settings, ((Tile, Int), (Tile, Int)), (Double, Double)) = {
    var raster = getRaster(globalSettings)
    if(!globalSettings.fromFile){
      val actualLat = ((globalSettings.latMax-globalSettings.latMin)/(10 + 990/runs *i)).toInt
      val actualLon = ((globalSettings.lonMax-globalSettings.lonMin)/(10 + 990/runs *i)).toInt
      globalSettings.sizeOfRasterLat = actualLat
      globalSettings.sizeOfRasterLon = actualLon
      raster = raster.resample(actualLat,actualLon)
    }

    //val image = new TileVisualizer()
    //image.visualTileNew(raster, globalSettings, "plainRaster")
    val (para: Settings, paraChild: Settings) = getParentChildSetting(globalSettings)
    val score = gStar(raster, para, paraChild)
    val chs = ((new ClusterHotSpots(score._1)).findClusters(para.clusterRange, para.critivalValue),
      (new ClusterHotSpots(score._2)).findClusters(paraChild.clusterRange, paraChild.critivalValue))

    visulizeCluster((para,paraChild), chs)
    val soh = new SoH()
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(chs)
    (para, paraChild, chs, sohVal)
  }

  def getParentChildSetting(global : Settings): (Settings, Settings) = {
    val para = new Settings()
    para.weightRadius = 3
    para.focalRange = global.focalRange
    para.sizeOfRasterLat = global.sizeOfRasterLat
    para.sizeOfRasterLon = global.sizeOfRasterLon
    para.focal = global.focal
    val paraChild = new Settings()
    paraChild.focal = global.focal
    paraChild.focalRange = global.focalRange
    paraChild.sizeOfRasterLat = global.sizeOfRasterLat
    paraChild.sizeOfRasterLon = global.sizeOfRasterLon
    paraChild.parent = false
    paraChild.weightRadius = 2
    (para, paraChild)
  }

  def saveSoHResults(totalTime: Long, outPutResults: ListBuffer[SoHResult], para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      para,
      paraChild,
      ((System.currentTimeMillis() - totalTime) / 1000),
      sohVal)
    println(outPutResultPrinter.printResults(outPutResults))
  }

  def visulizeCluster(para: (Settings, Settings), chs: ((Tile, Int), (Tile, Int))): Unit = {
    val image = new TileVisualizer()
    image.visualTileNew(chs._1._1, para._1, "clusterParent")
    image.visualTileNew(chs._2._1, para._2, "clusterChild")
  }

  def gStar(tile : Tile, paraParent : parmeters.Settings, child : parmeters.Settings): (Tile, Tile) = {
    var startTime = System.currentTimeMillis()
    var ord : GetisOrd = null
    if(paraParent.focal){
      ord = new GetisOrdFocal(tile, paraParent)
    } else {
      ord = new GetisOrd(tile, paraParent)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()

    val score =ord.getGstartForChildToo(paraParent, child)


    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    val image = new TileVisualizer()
    startTime = System.currentTimeMillis()
    image.visualTileNew(score._1, paraParent, "gStar")
    image.visualTileNew(score._2, child, "gStar")
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
    println("Time for RasterTransformation =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + arrayTile.cols + "," + arrayTile.rows + ")")
    arrayTile
  }


}
