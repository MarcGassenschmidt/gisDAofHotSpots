package scenarios

import java.io.{File, PrintWriter}
import java.time.LocalDateTime

import importExport.ImportGeoTiff
import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd.{GetisOrd, GetisOrdFocal, SoH, Weight}
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.joda.time.DateTime
/**
  * Created by marc on 24.05.17.
  */
class GenericScenario extends LazyLogging {

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
    val pw = new PrintWriter(new File(dir+LocalDateTime.now().formatted("dd_MM___HH_mm_")+"result.csv"))
    outPutResultPrinter.printResultsList(outPutResults)
    outPutResults.map(x => pw.println(x.format(false)))
    pw.flush()
    pw.close()
    val pwShort = new PrintWriter(new File(dir+LocalDateTime.now().formatted("dd_MM___HH_mm_")+"short_result.csv"))
    outPutResultPrinter.printResults(outPutResults,true, pwShort)
    pwShort.flush()
    pwShort.close()
  }

  def saveSoHResults(totalTime: Long, outPutResults: ListBuffer[SoHResult], globalSettings: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double), lat : (Int,Int)): Unit = {
    val outPutResultPrinter = new SoHResultTabell()
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      globalSettings,
      ((System.currentTimeMillis() - totalTime) / 1000),
      sohVal,
      lat._1)
    val dir = globalSettings.ouptDirectory+globalSettings.scenario+"/"
    val pwShort = new PrintWriter(new File(dir+"focal_"+globalSettings.focal+"d3.csv"))
    pwShort.println("F,W,Z,Down,Up")
    pwShort.println(outPutResultPrinter.getFormatedResultsListShort(outPutResults))
    pwShort.flush()
    pwShort.close()
    println("grepTextStart--------------------------------------")
    outPutResultPrinter.printResultsList(outPutResults)
    println("grepTextEnd--------------------------------------")

    println(outPutResultPrinter.printResults(outPutResults,true))
  }

  def visulizeCluster(setting: Settings, chs: ((Tile, Int), (Tile, Int)), first : Boolean): Unit = {
    val image = new TileVisualizer()
    if(first){
      image.visualTileNew(chs._1._1, setting, "cluster")
    }
    image.visualTileNew(chs._2._1, setting, "cluster")
  }

  def visulizeCluster(setting: Settings, clusters: Tile): Unit = {
    val image = new TileVisualizer()
    image.visualTileNew(clusters, setting, "cluster")
  }

  def gStar(tile : Tile, settings : parmeters.Settings, visualize : Boolean): Tile = {
    var startTime = System.currentTimeMillis()
    var ord : GetisOrd = null
    if(settings.focal){
      ord = new GetisOrdFocal(tile, settings)
    } else {
      ord = new GetisOrd(tile, settings)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    val score =ord.gStarComplete()
    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))

    if(visualize){
      startTime = System.currentTimeMillis()
      val image = new TileVisualizer()
      image.visualTileNew(score, settings, "gStar")
      println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    }
    score
  }


  def getRaster(settings : Settings): Tile = {
    val serializer = new SerializeTile(settings.serilizeDirectory)
    var raster : Tile = null
    if(settings.fromFile){
      raster = creatRaster(settings)
      serializer.write(raster)
    } else {
      raster = serializer.read()
    }
    aggregateToZoom(raster, settings.zoomLevel)
  }

  def aggregateToZoom(tile : Tile, zoomLevel : Int) : Tile = {
    val result : Tile = tile.downsample(tile.cols/zoomLevel, tile.rows/zoomLevel)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    result
  }

  def aggregateTile(tile : Tile): Tile ={
    val result : Tile = tile.downsample(tile.cols/2, tile.rows/2)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    result
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
    for (i <- 5 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = false
      if(i==5){
        globalSettings.fromFile = true
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double),  lat : (Int,Int)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
    }
  }

  def forFocalG(globalSettings: Settings, outPutResults: ListBuffer[SoHResult], runs: Int): Unit = {
    for (i <- 5 to runs) {
      var totalTime = System.currentTimeMillis()
      globalSettings.focal = true
      globalSettings.focalRange = 30
      if(i==5){
        globalSettings.fromFile = true
      } else {
        globalSettings.fromFile = false
      }
      val (para: Settings, chs: ((Tile, Int), (Tile, Int)), sohVal: (Double, Double), lat : (Int,Int)) = oneCase(globalSettings, i, runs)
      saveSoHResults((System.currentTimeMillis() - totalTime) / 1000, outPutResults, para, chs, sohVal, lat)
    }
  }

  def oneCase(globalSettings: Settings, i : Int, runs : Int): (Settings, ((Tile, Int), (Tile, Int)), (Double, Double), (Int, Int)) = {
    val raster : Tile = getRasterFromGeoTiff(globalSettings, i, runs, 0, "raster", getRasterWithCorrectResolution(globalSettings, i, runs, 0)._1)
    val raster_plus1 = getRasterFromGeoTiff(globalSettings, i, runs, 1, "raster", getRasterWithCorrectResolution(globalSettings, i, runs, 1)._1)

    val gStarParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "gStar", gStar(raster, globalSettings, i==0))
    val gStarChild = getRasterFromGeoTiff(globalSettings, i, runs, 1, "gStar", gStar(raster_plus1, globalSettings, i<runs))

    println("G* End")

    val clusterParent = getRasterFromGeoTiff(globalSettings, i, runs, 0, "cluster",((new ClusterHotSpots(gStarParent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue))._1)
    val clusterChild =getRasterFromGeoTiff(globalSettings, i, runs, 1, "cluster", (new ClusterHotSpots(gStarChild)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)._1)
    val time = System.currentTimeMillis()
    val numberclusterParent = clusterParent.findMinMax._2
    val numberclusterChild = clusterChild.findMinMax._2
    System.out.println("Time for Number of Cluster:"+(System.currentTimeMillis()-time)/1000)
    println("End Cluster")
    visulizeCluster(globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), i==0)
    println("End Visual Cluster")
    val soh = new SoH()
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(clusterParent,clusterChild)
    (globalSettings, ((clusterParent,numberclusterParent),(clusterChild,numberclusterChild)), sohVal,
      ((10.0 + 990.0 / runs.toDouble * i).ceil.toInt, //Just lat for export
        (10.0 + 990.0 / runs.toDouble * i +1).ceil.toInt)) //Just lat for export
  }

  def getRasterWithCorrectResolution(globalSettings: Settings, i : Int, runs : Int, next : Int): (Tile,Int,Int) = {
    val actualLat = ((globalSettings.latMax - globalSettings.latMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    val actualLon = ((globalSettings.lonMax - globalSettings.lonMin) / (10.0 + 990.0 / runs.toDouble * i + next)).ceil.toInt
    globalSettings.sizeOfRasterLat = actualLat
    globalSettings.sizeOfRasterLon = actualLon
    var raster_plus1 = getRaster(globalSettings)
    if (!globalSettings.fromFile) {
      raster_plus1 = raster_plus1.resample(actualLat, actualLon)
      println("Raster Size (cols,rows)=(" + raster_plus1.cols + "," + raster_plus1.rows + ")")
    }
    (raster_plus1,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt,(10.0 + 990.0 / runs.toDouble * i + next).ceil.toInt)
  }

  def getRasterFromGeoTiff(globalSettings : Settings, i : Int, runs : Int, next : Int, extra : String, tileFunction :  => Tile): Tile = {
    val importer = new ImportGeoTiff()
    if (false && globalSettings.fromFile && importer.geoTiffExists(globalSettings, i+next, runs, extra)) {
      return importer.getGeoTiff(globalSettings, i+next, runs, extra)
    } else {
      val tile = tileFunction
      importer.writeGeoTiff(tile, globalSettings, i+next, runs, extra)

      return tile
    }
  }



}
