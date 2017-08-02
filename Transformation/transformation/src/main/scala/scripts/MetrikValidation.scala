package scripts

import java.io.File

import clustering.{ClusterHotSpotsTime, ClusterRelations}
import com.typesafe.scalalogging.Logger
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import getisOrd.SoH.SoHResults
import getisOrd.{GetisOrd, SoH, TimeGetisOrd, Weight}
import importExport._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
    val top = (40.769239 + 0.010368, -73.948286 + 0.008021)
    settings.scenario = Scenario.Time.toString
    settings.shiftToPostive = -1 * buttom._2 * multiToInt
    settings.latMin = buttom._1 * multiToInt
    settings.lonMin = buttom._2 * multiToInt + settings.shiftToPostive
    settings.latMax = top._1 * multiToInt
    settings.lonMax = top._2 * multiToInt + settings.shiftToPostive
    settings.aggregationLevel = 4
    settings.sizeOfRasterLat = settings.aggregationLevel*100 //meters
    settings.sizeOfRasterLon = settings.aggregationLevel*100 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt

    settings.weightRadius = 10
    settings.weightRadiusTime = 1

    settings.focal = false
    settings.focalRange = settings.weightRadius + 20
    settings.focalRangeTime = 2


    val importTer = new ImportGeoTiff()

    settings.csvMonth = 1
    settings.csvYear = 2016
    writeBand(settings, importTer)


    val origin = importTer.getMulitGeoTiff(settings, TifType.Raw)
    assert(origin.cols % 4 == 0 && origin.rows % 4 == 0)
    settings.layoutTileSize = ((origin.cols / 4.0).floor.toInt, (origin.rows / 4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(settings)

    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))


    //----------------------------------GStar----------------------------------
    settings.focal = false
    writeOrGetGStar(rdd,settings,origin,importTer)
    //----------------------------------GStar-End---------------------------------

    //println("deb1")

    //---------------------------------Calculate Metrik----------------------------------
    StringWriter.writeFile(writeExtraMetrikRasters(settings, importTer, origin, rdd).toString,ResultType.Metrik,settings)
    //---------------------------------Calculate Metrik-End---------------------------------

    println("deb2")

    //---------------------------------Validate-Focal-GStar----------------------------------
    validate(settings, importTer)
    settings.csvMonth = 1
    settings.csvYear = 2016
    //---------------------------------Validate-Focal-GStar----------------------------------

    println("deb3")

    //---------------------------------Cluster-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-GStar-End---------------------------------

    //---------------------------------Focal-GStar----------------------------------
    settings.focal = true
    writeOrGetGStar(rdd,settings,origin,importTer)
    //---------------------------------Focal-GStar-End---------------------------------

    println("deb4")

    //---------------------------------Calculate Metrik----------------------------------
    StringWriter.writeFile(writeExtraMetrikRasters(settings, importTer, origin, rdd).toString,ResultType.Metrik,settings)
    //---------------------------------Calculate Metrik-End---------------------------------

    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------

    println("deb5")

    //---------------------------------Validate-Focal-GStar----------------------------------
    validate(settings, importTer)
    //---------------------------------Validate-Focal-GStar----------------------------------
  }

  def validate(settings: Settings, imporTer: ImportGeoTiff): Unit = {
    val gStar = imporTer.getMulitGeoTiff(settings, TifType.GStar)
    val mbT = (new ClusterHotSpotsTime(gStar)).findClusters()
    val relation = new ClusterRelations()
    val array = new Array[Double](4)
    settings.csvYear = 2012
    for (i <- 0 to 2) {
      var compare: MultibandTile = null
      if (PathFormatter.exist(settings, TifType.Cluster)) {
        compare = imporTer.getMulitGeoTiff(settings, TifType.Cluster)
      } else {
        writeBand(settings, imporTer)
        val origin = imporTer.getMulitGeoTiff(settings, TifType.Raw)
        val rdd = imporTer.repartitionFiles(settings)
        val gStarCompare = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
        imporTer.writeMultiGeoTiff(gStarCompare, settings, TifType.GStar)
        compare = (new ClusterHotSpotsTime(gStarCompare)).findClusters()
        imporTer.writeMultiGeoTiff(compare, settings, TifType.Cluster)
      }
      settings.csvYear += 1

      array(i) = relation.getPercentualFitting(mbT, compare)
    }
    var res = ""
    array.map(x => res+=x+"\n")
    StringWriter.writeFile(res,ResultType.Validation,settings)

  }

  def writeOrGetGStar(rdd: RDD[(SpatialKey, MultibandTile)], settings: Settings, origin: MultibandTile, importGeoTiff: ImportGeoTiff): MultibandTile = {
    if (PathFormatter.exist(settings, TifType.GStar)) {
      return importGeoTiff.getMulitGeoTiff(settings, TifType.GStar)
    } else {
      val gStar = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      importGeoTiff.writeMultiGeoTiff(gStar, settings, TifType.GStar)
      return gStar
    }
  }

  def getNeighbours(settings: Settings,
                    importTer: ImportGeoTiff,
                    origin: MultibandTile,
                    rdd: RDD[(SpatialKey, MultibandTile)]
                   ): ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)) = {

    var focalP: MultibandTile = null
    var focalN: MultibandTile = null

    var weightP: MultibandTile = null
    var weightN: MultibandTile = null

    var aggregateP: MultibandTile = null
    var aggregateN: MultibandTile = null

    //----------------------------Focal--P------------------------
    println("deb.01")
    settings.focalRange += 1
    focalP = writeOrGetGStar(rdd, settings, origin, importTer)
    //----------------------------Focal--N------------------------
    println("deb.02")
    settings.focalRange -= 2
    focalN = writeOrGetGStar(rdd, settings, origin, importTer)
    settings.focalRange += 1

    //----------------------------Weight-P-------------------------
    println("deb.03")
    settings.weightRadius += 1
    weightP = writeOrGetGStar(rdd, settings, origin, importTer)
    //----------------------------Weight-N-------------------------
    println("deb.04")
    settings.weightRadius -= 2
    weightN = writeOrGetGStar(rdd, settings, origin, importTer)
    settings.weightRadius += 1

    //----------------------------Aggregation P--------------------------
    println("deb.05")
    settings.aggregationLevel += 1
    settings.sizeOfRasterLat = settings.sizeOfRasterLat*2 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon*2 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand(settings, importTer)
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      aggregateP = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(aggregateP, settings, TifType.GStar)
    } else {
      aggregateP = writeOrGetGStar(rdd, settings, origin, importTer)
    }
    //----------------------------Aggregation N--------------------------
    println("deb.06")
    settings.aggregationLevel -= 2
    settings.sizeOfRasterLat = settings.sizeOfRasterLat/4 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon/4 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt

    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand(settings, importTer)
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      aggregateN = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(aggregateN, settings, TifType.GStar)

    } else {
      aggregateN = writeOrGetGStar(rdd, settings, origin, importTer)
    }
    settings.aggregationLevel += 1
    settings.sizeOfRasterLat = 2*settings.sizeOfRasterLat //meters
    settings.sizeOfRasterLon = 2*settings.sizeOfRasterLon //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt

    ((focalP, focalN), (weightP, weightN), (aggregateP, aggregateN))
  }

  def getClusterNeighbours(neighbours: ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)))
  : ((MultibandTile, MultibandTile), (MultibandTile, MultibandTile), (MultibandTile, MultibandTile)) = {
    (((new ClusterHotSpotsTime(neighbours._1._1)).findClusters(),
      (new ClusterHotSpotsTime(neighbours._1._2)).findClusters()),

      ((new ClusterHotSpotsTime(neighbours._2._1)).findClusters(),
        (new ClusterHotSpotsTime(neighbours._2._2)).findClusters()),

      ((new ClusterHotSpotsTime(neighbours._3._1)).findClusters(),
        (new ClusterHotSpotsTime(neighbours._3._2)).findClusters()))
  }

  def getMonthTile(settings: Settings, imporTer: ImportGeoTiff): Tile = {
    if (PathFormatter.exist(settings, TifType.GStar,false)) {
      return imporTer.readGeoTiff(settings, TifType.GStar)
    }
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    val res = GenericScenario.gStar(arrayTile, settings, false)
    imporTer.writeGeoTiff(res, settings, TifType.GStar)
    res
  }

  def getMonthTileGisCup(settings: Settings, imporTer: ImportGeoTiff, origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): MultibandTile = {
    settings.weightRadiusTime = 1
    settings.weightRadius = 1
    settings.weightMatrix = Weight.One
    if (PathFormatter.exist(settings, TifType.GStar)) {
      return imporTer.getMulitGeoTiff(settings, TifType.GStar)
    }
    val res = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    settings.weightRadiusTime = 1
    settings.weightRadius = 10
    settings.weightMatrix = Weight.Square
    res
  }

  def writeExtraMetrikRasters(settings: Settings, importTer: ImportGeoTiff, origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): SoHResults = {
    val neighbours = getNeighbours(settings: Settings, importTer: ImportGeoTiff, origin, rdd)
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    val clusterNeighbours = getClusterNeighbours(neighbours)
    val month: Tile = getMonthTile(settings, importTer)
    val gisCups = getMonthTileGisCup(settings,importTer,origin,rdd)
    SoH.getMetrikResults(gStar,
      clusterHotspots(settings, importTer),
      clusterNeighbours._3,
      clusterNeighbours._2,
      clusterNeighbours._1,
      month,
      settings,
      (new ClusterHotSpotsTime(gisCups)).findClusters())

  }

  def clusterHotspots(settings: Settings, importTer: ImportGeoTiff): MultibandTile = {
    if (PathFormatter.exist(settings, TifType.Cluster)) {
      importTer.getMulitGeoTiff(settings, TifType.Cluster)
    }
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(gStar)
    val hotSpots = clusterHotSpotsTime.findClusters()
    (new ImportGeoTiff().writeMultiGeoTiff(hotSpots, settings, TifType.Cluster))
    return hotSpots
  }



  def writeBand(settings: Settings, importTer: ImportGeoTiff): Unit = {
    if (PathFormatter.exist(settings, TifType.Raw)) {
      return
    }
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, TifType.Raw)
  }

}
