package parmeters

import getisOrd.Weight
import org.apache.spark.SparkConf

/**
  * Created by marc on 12.05.17.
  */
class Settings extends Serializable with Cloneable{
  var zoomLevel = 1

  var scenario = Scenario.NoScenario
  var multiToInt = 1000000
  //40.701915, -74.018704
  //40.763458, -73.967244
  val buttom = (40.699607, -74.020265)
  val top = (40.769239, -73.948286)
  var shiftToPostive = -1*buttom._2*multiToInt
  var latMin = buttom._1*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
  var lonMin = buttom._2*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
  var latMax = top._1*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
  var lonMax = top._2*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
  var sizeOfRasterLat = 400 //meters
  var sizeOfRasterLon = 400 //meters
  var rasterLatLength = ((latMax-latMin)/sizeOfRasterLat).ceil.toInt
  var rasterLonLength = ((lonMax-lonMin)/sizeOfRasterLon).ceil.toInt
  var weightMatrix = Weight.Square
  var weightRadius = 10
  var weightRadiusTime = 3
  var fromFile = false
  var clusterRange = 1.0
  var critivalValue = 5
  var focal = false
  var focalRange = weightRadius+10
  var focalRangeTime = weightRadiusTime+1
  var parent = true
  var inputDirectory = "/home/marc/Masterarbeit/outPut/raster"

  //var serilizeDirectory = "/home/marc/Masterarbeit/outPut/raster"
  var serilizeDirectory = "/home/marc/media/SS_17/output/raster"
  var statDirectory = "/home/marc/media/SS_17/output/"

  //var ouptDirectory = "/home/marc/Masterarbeit/outPut/"
  var ouptDirectory = "/home/marc/media/SS_17/output/"

  //var inputDirectoryCSV = "/home/marc/Downloads/in.csv"
  var inputDirectoryCSV = "/home/media/Downloads/"
  var csvMonth = 5
  var csvYear = 2011
  var hour = 0
  var spaceTime = true

  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[*]")
  object Scenario extends Enumeration {
    val NoScenario,Weight,Aggregation,Focal,Time,Presentation,Script = Value
  }

}
