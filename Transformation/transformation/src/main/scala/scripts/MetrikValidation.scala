package scripts

import clustering.ClusterHotSpotsTime
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import getisOrd.SoH.SoHResults
import getisOrd.{GetisOrd, SoH, TimeGetisOrd}
import importExport.{ImportGeoTiff, PathFormatter}
import org.apache.spark.rdd.RDD
import parmeters.{Scenario, Settings}
import rasterTransformation.Transformation
import scenarios.GenericScenario
import timeUtils.MultibandUtils

/**
  * Created by marc on 19.07.17.
  */
object MetrikValidation {

  def main(args: Array[String]): Unit = {
    val settings = new Settings()

    var multiToInt = 1000000
    val buttom = (40.699607, -74.020265)
    val top = (40.769239+0.010368, -73.948286+0.008021)
    settings.scenario = Scenario.Time.toString
    settings.shiftToPostive = -1*buttom._2*multiToInt
    settings.latMin = buttom._1*multiToInt
    settings.lonMin = buttom._2*multiToInt+settings.shiftToPostive
    settings.latMax = top._1*multiToInt
    settings.lonMax = top._2*multiToInt+settings.shiftToPostive
    settings.sizeOfRasterLat = 400 //meters
    settings.sizeOfRasterLon = 400 //meters
    settings.rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon).ceil.toInt


    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "MetrikValidations")
    println(dir)
    val importTer = new ImportGeoTiff()

    writeBand(settings, dir, importTer)
    val origin = importTer.getMulitGeoTiff(dir+"firstTimeBand.tif",settings)
    assert(origin.cols % 4==0 && origin.rows % 4==0)
    settings.layoutTileSize = ((origin.cols/4.0).floor.toInt,(origin.rows/4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)

    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))


    //----------------------------------GStar----------------------------------
    settings.focal = false
    getGstar(settings, dir, importTer, origin, rdd)
    //----------------------------------GStar-End---------------------------------

    //---------------------------------Calculate Metrik----------------------------------
    println(writeExtraMetrikRasters(settings,dir,importTer,origin,rdd).toString)
    //---------------------------------Calculate Metrik-End---------------------------------

    //---------------------------------Cluster-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-GStar-End---------------------------------

    //---------------------------------Focal-GStar----------------------------------
    settings.focal = true
    getGstar(settings, dir, importTer, origin, rdd)
    //---------------------------------Focal-GStar-End---------------------------------
    println(writeExtraMetrikRasters(settings,dir,importTer,origin,rdd).toString)
    //---------------------------------Calculate Metrik----------------------------------

    //---------------------------------Calculate Metrik-End---------------------------------

    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------

    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------
  }

  def getNeighbours(settings: Settings, s: String,
                    importTer: ImportGeoTiff,
                    origin: MultibandTile,
                    rdd: RDD[(SpatialKey, MultibandTile)]): ((MultibandTile,MultibandTile),(MultibandTile,MultibandTile),(MultibandTile,MultibandTile)) = {
    settings.focalRange += 1
    val focalP = TimeGetisOrd.getGetisOrd(rdd,settings,origin)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(focalP)
    var hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)

    settings.focalRange -= 2
    val focalN = TimeGetisOrd.getGetisOrd(rdd,settings,origin)
    clusterHotSpotsTime = new ClusterHotSpotsTime(focalN)
    hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
    settings.focalRange +=1

    settings.weightRadius += 1
    val weightP = TimeGetisOrd.getGetisOrd(rdd,settings,origin)
    clusterHotSpotsTime = new ClusterHotSpotsTime(focalP)
    hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)

    settings.weightRadius -= 2
    val weightN = TimeGetisOrd.getGetisOrd(rdd,settings,origin)
    clusterHotSpotsTime = new ClusterHotSpotsTime(weightP)
    hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
    settings.weightRadius +=1

    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "MetrikValidations")

    val a1 = MultibandUtils.aggregateToZoom(origin,settings.zoomLevel+1)
    importTer.writeMultiGeoTiff(a1, settings, dir + "firstTimeBandP.tif")
    val rdd1 = importTer.repartitionFiles(dir+"firstTimeBandP.tif", settings)
    val aggregateP = TimeGetisOrd.getGetisOrd(rdd1,settings,a1)


    settings.sizeOfRasterLat = settings.sizeOfRasterLat/2 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon/2 //meters
    settings.rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon).ceil.toInt
    writeBand(settings,dir,importTer)
    val a2 = importTer.getMulitGeoTiff(dir+"firstTimeBand.tif",settings)
    val rdd2 = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)
    val aggregateN = TimeGetisOrd.getGetisOrd(rdd2,settings,a2)


    settings.sizeOfRasterLat = settings.sizeOfRasterLat*2 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon*2 //meters
    settings.rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon).ceil.toInt

    ((focalP,focalN),(weightP,weightN),(aggregateP,aggregateN))
  }

  def getClusterNeighbours(neighbours: ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)))
                  : ((MultibandTile,MultibandTile),(MultibandTile,MultibandTile),(MultibandTile,MultibandTile)) = {
    (((new ClusterHotSpotsTime(neighbours._1._1)).findClusters(),
    (new ClusterHotSpotsTime(neighbours._1._2)).findClusters()),

    ((new ClusterHotSpotsTime(neighbours._2._1)).findClusters(),
    (new ClusterHotSpotsTime(neighbours._2._2)).findClusters()),

    ((new ClusterHotSpotsTime(neighbours._3._1)).findClusters(),
    (new ClusterHotSpotsTime(neighbours._3._2)).findClusters()))
  }

  def getMonthTile(settings: Settings) : Tile = {
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    GenericScenario.gStar(arrayTile,settings,false)
  }

  def writeExtraMetrikRasters(settings: Settings, dir: String, importTer: ImportGeoTiff, origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): SoHResults ={
    val neighbours = getNeighbours(settings: Settings, dir: String, importTer: ImportGeoTiff,origin,rdd)
    val gStar = importTer.getMulitGeoTiff(dir + "gStar.tif", settings)
    val clusterNeighbours = getClusterNeighbours(neighbours)
    val month : Tile= getMonthTile(settings)
    SoH.getMetrikResults(gStar,
                        neighbours._2._2,
                        (new ClusterHotSpotsTime(gStar)).findClusters(),
                        clusterNeighbours._3,
                        clusterNeighbours._2,
                        clusterNeighbours._1,
                        month,
                        settings)

  }

  def clusterHotspots(settings: Settings, dir: String, importTer: ImportGeoTiff): Unit = {
    val gStar = importTer.getMulitGeoTiff(dir + "gStar.tif", settings)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(gStar)
    val hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
    //println(hotSpots._1.band(10).resample(50,50).asciiDraw())
    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(hotSpots._1,settings,dir+"ClustergStar.tif"))
    (new ImportGeoTiff().writeMultiGeoTiff(hotSpots._1, settings, dir + "ClustergStar.tif"))
  }

  def getGstar(settings: Settings, dir: String, importTer: ImportGeoTiff, origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): Unit = {
    //println(dir + "gStar.tif")
    var r2 = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    importTer.writeMultiGeoTiff(r2, settings, dir + "gStar.tif")
  }

  def writeBand(settings: Settings, dir: String, importTer: ImportGeoTiff): Unit = {
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, dir + "firstTimeBand.tif")
  }

}
