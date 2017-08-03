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

  def defaultSetting(): Settings ={
    getBasicSettings(10,1,30,2,4,1)
  }

  def getBasicSettings(weight : Int, weightTime : Int, focalWeight : Int, focalTime : Int, aggregationLevel : Int, month : Int): Settings = {
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
    settings.aggregationLevel = aggregationLevel
    settings.sizeOfRasterLat = settings.aggregationLevel * 100 //meters
    settings.sizeOfRasterLon = settings.aggregationLevel * 100 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt

    settings.weightRadius = weight
    settings.weightRadiusTime = weightTime

    settings.focal = false
    settings.focalRange = focalWeight
    settings.focalRangeTime = focalTime
    settings.csvMonth = month
    settings.csvYear = 2016
    settings
  }

  def main(args: Array[String]): Unit = {
    val validation = new MetrikValidation()
    val monthToTest = 2 //1 to 3
    val weightToTest = 2
    val weightStepSize = 1 //10 to 10+2*weightToTest
    val focalRangeToTest = 2
    val focalRangeStepSize = 2 //30 to 30+2*focalRangeToTest
    val timeDimensionStep = 2
    val aggregationSteps = 2 //400, 800
    val experiments = new Array[Settings](monthToTest*weightToTest*focalRangeToTest*timeDimensionStep*aggregationSteps)
    var counter = 0
    for(m <- 0 to monthToTest-1){
      for(w <- 0 to weightToTest-1){
        for(f <- 0 to focalRangeToTest-1) {
          for(a <- 0 to aggregationSteps-1) {
            for(tf <- 0 to timeDimensionStep-1) {
              for(tw <- 0 to timeDimensionStep-1) {
                experiments(counter) = getBasicSettings(10 + w * weightStepSize, 1+tw, 30 + focalRangeToTest * focalRangeStepSize, 2+tf, 4 + a, 1 + m)
                counter += 1
              }
            }
          }
        }
      }
    }
    for(setting <- experiments){
      println("Start of experiment:"+setting.toString)
      validation.oneTestRun(setting)
    }

  }
}
class MetrikValidation {
  var settings = new Settings
  val importTer = new ImportGeoTiff()
  def oneTestRun(secenarioSettings: Settings): Unit = {
    settings = secenarioSettings
    writeBand()
    val origin = importTer.getMulitGeoTiff(settings, TifType.Raw)
    assert(origin.cols % 4 == 0 && origin.rows % 4 == 0)
    settings.layoutTileSize = ((origin.cols / 4.0).floor.toInt, (origin.rows / 4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(settings)
    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))
    //----------------------------------GStar----------------------------------
    settings.focal = false
    writeOrGetGStar(rdd, origin)
    //----------------------------------GStar-End---------------------------------
    println("deb1")
    //---------------------------------Calculate Metrik----------------------------------
    StringWriter.writeFile(writeExtraMetrikRasters(origin, rdd).toString, ResultType.Metrik, settings)
    //---------------------------------Calculate Metrik-End---------------------------------
    println("deb2")
    //---------------------------------Validate-Focal-GStar----------------------------------
    validate()
    //---------------------------------Validate-Focal-GStar----------------------------------
    println("deb3")
    //---------------------------------Cluster-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-GStar-End---------------------------------
    //---------------------------------Focal-GStar----------------------------------
    settings.focal = true
    writeOrGetGStar(rdd, origin)
    //---------------------------------Focal-GStar-End---------------------------------
    println("deb4")
    //---------------------------------Calculate Metrik----------------------------------
    StringWriter.writeFile(writeExtraMetrikRasters(origin, rdd).toString, ResultType.Metrik, settings)
    //---------------------------------Calculate Metrik-End---------------------------------
    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------
    println("deb5")
    //---------------------------------Validate-Focal-GStar----------------------------------
    validate()
    //---------------------------------Validate-Focal-GStar----------------------------------
  }

  def validate(): Unit = {
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    val mbT = (new ClusterHotSpotsTime(gStar)).findClusters()
    val relation = new ClusterRelations()
    val array = new Array[Double](4)
    val old = settings.csvYear
    settings.csvYear = 2011
    for (i <- 0 to 4) {
      var compare: MultibandTile = null
      if (PathFormatter.exist(settings, TifType.Cluster)) {
        compare = importTer.getMulitGeoTiff(settings, TifType.Cluster)
      } else {
        writeBand()
        val origin = importTer.getMulitGeoTiff(settings, TifType.Raw)
        val rdd = importTer.repartitionFiles(settings)
        val gStarCompare = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
        importTer.writeMultiGeoTiff(gStarCompare, settings, TifType.GStar)
        compare = (new ClusterHotSpotsTime(gStarCompare)).findClusters()
        importTer.writeMultiGeoTiff(compare, settings, TifType.Cluster)
      }
      settings.csvYear += 1

      array(i) = relation.getPercentualFitting(mbT, compare)
    }
    var res = ""
    array.map(x => res+=x+"\n")
    StringWriter.writeFile(res,ResultType.Validation,settings)
    settings.csvYear = old
  }

  def writeOrGetGStar(rdd: RDD[(SpatialKey, MultibandTile)], origin: MultibandTile): MultibandTile = {
    if (PathFormatter.exist(settings, TifType.GStar)) {
      return importTer.getMulitGeoTiff(settings, TifType.GStar)
    } else {
      val gStar = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      importTer.writeMultiGeoTiff(gStar, settings, TifType.GStar)
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
    focalP = writeOrGetGStar(rdd, origin)
    //----------------------------Focal--N------------------------
    println("deb.02")
    settings.focalRange -= 2
    focalN = writeOrGetGStar(rdd, origin)
    settings.focalRange += 1

    //----------------------------Weight-P-------------------------
    println("deb.03")
    settings.weightRadius += 1
    weightP = writeOrGetGStar(rdd, origin)
    //----------------------------Weight-N-------------------------
    println("deb.04")
    settings.weightRadius -= 2
    weightN = writeOrGetGStar(rdd, origin)
    settings.weightRadius += 1

    //----------------------------Aggregation P--------------------------
    println("deb.05")
    settings.aggregationLevel += 1
    settings.sizeOfRasterLat = settings.sizeOfRasterLat*2 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon*2 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand()
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      aggregateP = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(aggregateP, settings, TifType.GStar)
    } else {
      aggregateP = writeOrGetGStar(rdd, origin)
    }
    //----------------------------Aggregation N--------------------------
    println("deb.06")
    settings.aggregationLevel -= 2
    settings.sizeOfRasterLat = settings.sizeOfRasterLat/4 //meters
    settings.sizeOfRasterLon = settings.sizeOfRasterLon/4 //meters
    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt

    if (!PathFormatter.exist(settings, TifType.GStar)) {
      writeBand()
      val a2 = importTer.getMulitGeoTiff(settings, TifType.Raw)
      val rdd2 = importTer.repartitionFiles(settings)
      aggregateN = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      importTer.writeMultiGeoTiff(aggregateN, settings, TifType.GStar)

    } else {
      aggregateN = writeOrGetGStar(rdd, origin)
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

  def getMonthTile(): Tile = {
    if (PathFormatter.exist(settings, TifType.GStar,false)) {
      return importTer.readGeoTiff(settings, TifType.GStar)
    }
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    val res = GenericScenario.gStar(arrayTile, settings, false)
    importTer.writeGeoTiff(res, settings, TifType.GStar)
    res
  }

  def getMonthTileGisCup(origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): MultibandTile = {
    settings.weightRadiusTime = 1
    settings.weightRadius = 1
    settings.weightMatrix = Weight.One
    if (PathFormatter.exist(settings, TifType.GStar)) {
      return importTer.getMulitGeoTiff(settings, TifType.GStar)
    }
    val res = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    settings.weightRadiusTime = 1
    settings.weightRadius = 10
    settings.weightMatrix = Weight.Square
    res
  }

  def writeExtraMetrikRasters(origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): SoHResults = {
    val neighbours = getNeighbours(settings: Settings, importTer: ImportGeoTiff, origin, rdd)
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    val clusterNeighbours = getClusterNeighbours(neighbours)
    val month: Tile = getMonthTile()
    val gisCups = getMonthTileGisCup(origin,rdd)
    val cluster = clusterHotspots()
    StringWriter.writeFile(SoH.getPoints(cluster,settings),ResultType.HotSpots,settings)
    SoH.getMetrikResults(gStar,
      cluster,
      clusterNeighbours._3,
      clusterNeighbours._2,
      clusterNeighbours._1,
      month,
      settings,
      (new ClusterHotSpotsTime(gisCups)).findClusters())

  }

  def clusterHotspots(): MultibandTile = {
    if (PathFormatter.exist(settings, TifType.Cluster)) {
      importTer.getMulitGeoTiff(settings, TifType.Cluster)
    }
    val gStar = importTer.getMulitGeoTiff(settings, TifType.GStar)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(gStar)
    val hotSpots = clusterHotSpotsTime.findClusters()
    (new ImportGeoTiff().writeMultiGeoTiff(hotSpots, settings, TifType.Cluster))
    return hotSpots
  }



  def writeBand(): Unit = {
    if (PathFormatter.exist(settings, TifType.Raw)) {
      return
    }
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, TifType.Raw)
  }

}
