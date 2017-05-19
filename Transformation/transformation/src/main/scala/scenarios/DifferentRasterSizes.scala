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
    val totalTime = System.currentTimeMillis()
    val globalSettings =new Settings()
    val outPutResults = ListBuffer[SoHResult]()
    val raster = getRaster(globalSettings)
    val (para: Settings, paraChild: Settings) = getParentChildSetting

    val score = gStar(raster, para, paraChild)

    val chs = ((new ClusterHotSpots(score._1)).findClusters(para.critivalValue,para.critivalValue),
      (new ClusterHotSpots(score._2)).findClusters(paraChild.critivalValue,paraChild.critivalValue))

    visulizeCluster(para, chs)

    val soh = new SoH()
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(chs)

    saveSoHResults(totalTime, outPutResults, para, paraChild, chs, sohVal)
    println("Total Time ="+((System.currentTimeMillis()-totalTime)/1000))

  }

  def getParentChildSetting: (Settings, Settings) = {
    val para = new Settings()
    para.weightCols = 10
    para.weightRows = 10
    val paraChild = new Settings()
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
    image.visualTileNew(chs._1._1, para, "cluster")
    image.visualTileNew(chs._2._1, para, "cluster")
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
