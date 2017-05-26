package parmeters

import getisOrd.Weight
import org.apache.spark.SparkConf

/**
  * Created by marc on 12.05.17.
  */
class Settings extends Serializable{
  var scenario = "NotScenarioSet"
  var multiToInt = 1000000
  var shiftToPostive = 74.018704*multiToInt
  var latMin = 40.701915*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
  var lonMin = -74.018704*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
  var latMax = 40.763458*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
  var lonMax = -73.967244*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
  var sizeOfRasterLat = 10 //meters
  var sizeOfRasterLon = 10 //meters
  var rasterLatLength = ((latMax-latMin)/sizeOfRasterLat).ceil.toInt
  var rasterLonLength = ((lonMax-lonMin)/sizeOfRasterLon).ceil.toInt
  var weightMatrix = Weight.Square
  var weightRadius = 20
  var fromFile = false
  var clusterRange = 1
  var critivalValue = 2
  var focal = false
  var focalRange = 11
  var parent = true
  var inputDirectory = "/home/marc/Masterarbeit/outPut/raster"

  //var serilizeDirectory = "/home/marc/Masterarbeit/outPut/raster"
  var serilizeDirectory = "/home/marc/media/SS_17/output/raster"

  //var ouptDirectory = "/home/marc/Masterarbeit/outPut/"
  var ouptDirectory = "/home/marc/media/SS_17/output/"

  //var inputDirectoryCSV = "/home/marc/Downloads/in.csv"
  var inputDirectoryCSV = "/home/marc/media/Downloads/in.csv"

  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[*]")


}
