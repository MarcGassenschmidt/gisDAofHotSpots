package scripts

import java.io.File

import clustering.{ClusterHotSpotsTime, ClusterRelations}
import com.typesafe.scalalogging.Logger
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import getisOrd.SoH.SoHResults
import getisOrd.{GetisOrd, SoH, TimeGetisOrd, Weight}
import importExport.{ImportGeoTiff, PathFormatter}
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

    settings.weightRadius = 10
    settings.weightRadiusTime = 1

    settings.focal = false
    settings.focalRange = settings.weightRadius+20


    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "MetrikValidations")
    println(dir)
    val importTer = new ImportGeoTiff()

    settings.csvMonth = 1
    settings.csvYear = 2016
    //writeBand(settings, dir + "firstTimeBand.tif", importTer)


    val origin = importTer.getMulitGeoTiff(dir+"firstTimeBand.tif")
    assert(origin.cols % 4==0 && origin.rows % 4==0)
    settings.layoutTileSize = ((origin.cols/4.0).floor.toInt,(origin.rows/4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)

    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))


    //----------------------------------GStar----------------------------------
    settings.focal = false
    //getGstar(settings, dir, importTer, origin, rdd)
    //----------------------------------GStar-End---------------------------------

    //println("deb1")

    //---------------------------------Calculate Metrik----------------------------------
    //println(writeExtraMetrikRasters(settings,dir,importTer,origin,rdd).toString)
    //---------------------------------Calculate Metrik-End---------------------------------

    println("deb2")

    //---------------------------------Validate-Focal-GStar----------------------------------
    validate(settings,dir,importTer)
    settings.csvMonth = 1
    settings.csvYear = 2016
    //---------------------------------Validate-Focal-GStar----------------------------------

    println("deb3")

    //---------------------------------Cluster-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-GStar-End---------------------------------

    //---------------------------------Focal-GStar----------------------------------
    settings.focal = true
    //getGstar(settings, dir, importTer, origin, rdd)
    //---------------------------------Focal-GStar-End---------------------------------

    println("deb4")

    //---------------------------------Calculate Metrik----------------------------------
    //println(writeExtraMetrikRasters(settings,dir,importTer,origin,rdd).toString)
    //---------------------------------Calculate Metrik-End---------------------------------

    //---------------------------------Cluster-Focal-GStar----------------------------------
    //clusterHotspots(settings, dir, importTer)
    //---------------------------------Cluster-Focal-GStar-End---------------------------------

    println("deb5")

    //---------------------------------Validate-Focal-GStar----------------------------------
    validate(settings,dir,importTer)
    //---------------------------------Validate-Focal-GStar----------------------------------
  }

  def validate(settings :Settings, dir : String, imporTer : ImportGeoTiff): Unit ={
    val gStar = imporTer.getMulitGeoTiff(dir + "gStar.tif")
    val mbT =  (new ClusterHotSpotsTime(gStar)).findClusters()
    val path = new PathFormatter()
    val relation = new ClusterRelations()
    val array = new Array[Double](11)
    settings.csvYear = 2012
    for(i <- 0 to 1){

      val dir = path.getDirectory(settings, "MetrikValidations")

      writeBand(settings,dir+"validationGstar"+(settings.csvYear)+".tif",imporTer)
      val origin = imporTer.getMulitGeoTiff(dir+"validationGstar"+(settings.csvYear)+".tif")
      val rdd = imporTer.repartitionFiles(dir+"validationGstar"+(settings.csvYear)+".tif", settings)

      var compare : MultibandTile= null
      if(new File(dir+"validationCluster"+(settings.csvYear)+".tif").exists()){
        compare = imporTer.getMulitGeoTiff(dir+"validationCluster"+(settings.csvYear)+".tif")
      } else {
        val gStarCompare = TimeGetisOrd.getGetisOrd(rdd,settings,origin)
        compare =  (new ClusterHotSpotsTime(gStarCompare)).findClusters()
        imporTer.writeMultiGeoTiff(compare, settings ,dir+"validationCluster"+(settings.csvYear)+".tif")
      }
      settings.csvYear += 1

      array(i) = relation.getPercentualFitting(mbT,compare)
    }
    println("-----------------------------------------------------")
    println("Other year results are"+array)
    array.map(x=>println(x))
    println("-----------------------------------------------------")
  }

  def getNeighbours(settings: Settings, s: String,
                    importTer: ImportGeoTiff,
                    origin: MultibandTile,
                    rdd: RDD[(SpatialKey, MultibandTile)],
                    direc : String): ((MultibandTile,MultibandTile),(MultibandTile,MultibandTile),(MultibandTile,MultibandTile)) = {

    //var clusterHotSpotsTime : ClusterHotSpotsTime = null
    //var hotSpots : (MultibandTile,Int) = null
    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "MetrikValidations")
    var focalP : MultibandTile = null
    var focalN : MultibandTile = null

    var weightP : MultibandTile = null
    var weightN : MultibandTile = null

    var aggregateP : MultibandTile = null
    var aggregateN : MultibandTile = null

    //----------------------------Focal--P------------------------
    if((new File(direc + "gStarFPlus1.tif").exists)){
      focalP = importTer.getMulitGeoTiff(direc + "gStarFPlus1.tif")
    } else {
      println("deb.01")
      settings.focalRange += 1
      focalP = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(focalP)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(focalP, settings, direc + "gStarFPlus1.tif")
    }
    //----------------------------Focal--N------------------------
    if((new File(direc + "gStarFMinus1.tif").exists)){
      focalN = importTer.getMulitGeoTiff(direc + "gStarFMinus1.tif")
    } else {
      println("deb.02")
      settings.focalRange -= 2
      focalN = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(focalN)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(focalN, settings, direc + "gStarFMinus1.tif")
      settings.focalRange += 1
    }
    //----------------------------Weigth-P-------------------------
    if((new File(direc + "gStarWPlus1.tif").exists)){
      weightP = importTer.getMulitGeoTiff(direc + "gStarWPlus1.tif")
    } else {
      println("deb.03")
      settings.weightRadius += 1
      weightP = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(weightP)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(weightP, settings, direc + "gStarWPlus1.tif")
    }
    //----------------------------Weigth-N-------------------------
    if((new File(direc + "gStarWMinus1.tif").exists)){
      weightN = importTer.getMulitGeoTiff(direc + "gStarWMinus1.tif")
    } else {
      println("deb.04")
      settings.weightRadius -= 2
      weightN = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(weightP)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(weightP, settings, direc + "gStarWMinus1.tif")
      settings.weightRadius += 1
    }

    //----------------------------Aggregation P--------------------------
    if((new File(direc + "gStarAPlus.tif").exists)){
      aggregateP = importTer.getMulitGeoTiff(direc + "gStarAPlus.tif")
    } else {

      println("deb.05")
      val a1 = MultibandUtils.aggregateToZoom(origin, settings.zoomLevel + 1)
      importTer.writeMultiGeoTiff(a1, settings, dir + "firstTimeBandP.tif")
      val rdd1 = importTer.repartitionFiles(dir + "firstTimeBandP.tif", settings)
      aggregateP = TimeGetisOrd.getGetisOrd(rdd1, settings, a1)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(aggregateP)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(aggregateP, settings, direc + "gStarAPlus.tif")
    }
    //----------------------------Aggregation N--------------------------
    if((new File(direc + "gStarAMinus1.tif").exists)){
      aggregateN = importTer.getMulitGeoTiff(direc + "gStarAMinus1.tif")
    } else {
      println("deb.06")
      settings.sizeOfRasterLat = settings.sizeOfRasterLat / 2 //meters
      settings.sizeOfRasterLon = settings.sizeOfRasterLon / 2 //meters
      settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
      settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
      writeBand(settings, dir+ "firstTimeBandBigger.tif", importTer)
      val a2 = importTer.getMulitGeoTiff(dir + "firstTimeBandBigger.tif")
      val rdd2 = importTer.repartitionFiles(dir + "firstTimeBandBigger.tif", settings)
      aggregateN = TimeGetisOrd.getGetisOrd(rdd2, settings, a2)
      //clusterHotSpotsTime = new ClusterHotSpotsTime(aggregateN)
      //hotSpots = clusterHotSpotsTime.findClusters(1.9, 5)
      importTer.writeMultiGeoTiff(aggregateN, settings, direc + "gStarAMinus1.tif")
      println("deb.07")

      settings.sizeOfRasterLat = settings.sizeOfRasterLat * 2 //meters
      settings.sizeOfRasterLon = settings.sizeOfRasterLon * 2 //meters
      settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
      settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
    }
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

  def getMonthTile(settings: Settings, dir : String, imporTer : ImportGeoTiff) : Tile = {
    if((new File(dir+"month.tif").exists())){
      return imporTer.readGeoTiff(dir+"month.tif")
    }
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    val res = GenericScenario.gStar(arrayTile,settings,false)
    imporTer.writeGeoTiff(res,dir+"month.tif",settings)
    res
  }

  def getMonthTileGisCup(settings: Settings, dir : String, imporTer : ImportGeoTiff) : Tile = {
    if((new File(dir+"monthACM.tif").exists())){
      return imporTer.readGeoTiff(dir+"monthACM.tif")
    }
    assert(false)
    //TODO call it an set weight
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster(settings)
    val res = GenericScenario.gStar(arrayTile,settings,false)
    imporTer.writeGeoTiff(res,dir+"monthACM.tif",settings)
    res
  }

  def writeExtraMetrikRasters(settings: Settings, dir: String, importTer: ImportGeoTiff, origin: MultibandTile, rdd: RDD[(SpatialKey, MultibandTile)]): SoHResults ={
    val neighbours = getNeighbours(settings: Settings, dir: String, importTer: ImportGeoTiff,origin,rdd, dir)
    val gStar = importTer.getMulitGeoTiff(dir + "gStar.tif")
    val clusterNeighbours = getClusterNeighbours(neighbours)
    val month : Tile= getMonthTile(settings, dir, importTer)
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
    val gStar = importTer.getMulitGeoTiff(dir + "gStar.tif")
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
    if((new File(dir)).exists()){
      //Should not be reached
      //println("Rewrite file")
      return
    }
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, dir)
  }

}
