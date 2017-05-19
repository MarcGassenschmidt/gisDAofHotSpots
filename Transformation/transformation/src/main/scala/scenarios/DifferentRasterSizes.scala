package scenarios

import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd.{GetisOrd, GetisOrdFocal, SoH}
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 19.05.17.
  */
class DifferentRasterSizes {

  def runScenario(): Unit ={
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 2
    forGlobalG(globalSettings, outPutResults, runs)
    forFocalG(globalSettings, outPutResults, runs)
  }

  def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 1 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.sizeOfRasterLat = 10 + 990/runs  * i
      globalSettings.sizeOfRasterLon = 10 + 990/runs * i
      globalSettings.focal = true
      val (para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)) = oneCase(globalSettings)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, paraChild, chs, sohVal)
    }
  }

  def forGlobalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 1 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.sizeOfRasterLat = 10 + runs / 990 * i
      globalSettings.sizeOfRasterLon = 10 + runs / 990 * i
      globalSettings.focal = false
      val (para: Settings, paraChild: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double)) = oneCase(globalSettings)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, paraChild, chs, sohVal)
    }
  }

  def oneCase(globalSettings: Settings): (Settings, Settings, ((Tile, Int), (Tile, Int)), (Double, Double)) = {
    val raster = getRaster(globalSettings)
    val image = new TileVisualizer()
    image.visualTileNew(raster, globalSettings, "plainRaster")
    val (para: Settings, paraChild: Settings) = getParentChildSetting(globalSettings)
    //image.visualTileNew(raster.resample(20,20), globalSettings, "plainRasterResample")

    val score = gStar(raster, para, paraChild)

    val chs = ((new ClusterHotSpots(score._1)).findClusters(para.clusterRange, para.critivalValue),
      (new ClusterHotSpots(score._2)).findClusters(paraChild.clusterRange, paraChild.critivalValue))

    visulizeCluster(para, chs)
    val soh = new SoH()
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(chs)
    (para, paraChild, chs, sohVal)
  }

  def getParentChildSetting(global : Settings): (Settings, Settings) = {
    val para = new Settings()
    para.weightCols = 10
    para.weightRows = 10
    para.sizeOfRasterLat = global.sizeOfRasterLat
    para.sizeOfRasterLon = global.sizeOfRasterLon
    para.focal = global.focal
    val paraChild = new Settings()
    paraChild.focal = global.focal
    paraChild.sizeOfRasterLat = global.sizeOfRasterLat
    paraChild.sizeOfRasterLon = global.sizeOfRasterLon
    paraChild.parent = false
    paraChild.weightCols = 5
    paraChild.weightRows = 5
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

  def visulizeCluster(para: Settings, chs: ((Tile, Int), (Tile, Int))): Unit = {
    val image = new TileVisualizer()
    image.visualTileNew(chs._1._1, para, "clusterParent")
    image.visualTileNew(chs._2._1, para, "clusterChild")
  }

  def gStar(tile : Tile, paraParent : parmeters.Settings, child : parmeters.Settings): (Tile, Tile) = {
    var startTime = System.currentTimeMillis()
    var ort : GetisOrd = null
    if(paraParent.focal){
      ort = new GetisOrdFocal(tile, paraParent)
    } else {
      ort = new GetisOrd(tile, paraParent)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    var score =ort.getGstartForChildToo(paraParent, child)
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
