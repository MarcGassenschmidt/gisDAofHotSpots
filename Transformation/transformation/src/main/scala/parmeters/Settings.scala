package parmeters

import getisOrd.Weight
import org.apache.spark.SparkConf

/**
  * Created by marc on 12.05.17.
  */
class Settings extends Serializable{
  var multiToInt = 1000000
  var shiftToPostive = 74.018704*multiToInt
  var latMin = 40.701915*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
  var lonMin = -74.018704*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
  var latMax = 40.763458*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
  var lonMax = -73.967244*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
  var sizeOfRasterLat = 200 //meters
  var sizeOfRasterLon = 200 //meters
  var rasterLatLength = ((latMax-latMin)/sizeOfRasterLat).ceil.toInt
  var rasterLonLength = ((lonMax-lonMin)/sizeOfRasterLon).ceil.toInt
  var weightMatrix = Weight.Square
  var weightCols = 20
  var weightRows = 20
  var fromFile = false
  var clusterRange = 1
  var critivalValue = 2
  var focal = false
  var focalRange = 40
  var parent = true
  var inputDirectory = "/home/marc/Masterarbeit/outPut/raster"
  var ouptDirectory = "/home/marc/media/SS_17/output/"
  var serilizeDirectory = "/home/marc/media/SS_17/tmp/raster"
  var inputDirectoryCSV = "/home/marc/Masterarbeit/outPut/raster"
  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[*]")


}
