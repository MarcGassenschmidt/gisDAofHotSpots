package scripts

import importExport.ImportGeoTiff
import parmeters.Scenario
import rasterTransformation.Transformation

/**
  * Created by marc on 05.09.17.
  */
object ZoomScript {
  def main(args: Array[String]): Unit = {
    val csv = new Transformation
    val raster1 = csv.transformCSVtoTimeRaster(MetrikValidation.defaultSetting())
    val settings = MetrikValidation.defaultSetting()
//    var buttom = (40.699607, -74.020265 + add)
//    var top = (40.769239 + 0.010368 - add, -73.948286 + 0.008021 -add)
//    settings.scenario = Scenario.Time.toString
//    settings.shiftToPostive = -1 * buttom._2 * multiToInt
//    settings.latMin = buttom._1 * multiToInt
//    settings.lonMin = buttom._2 * multiToInt + settings.shiftToPostive
//    settings.latMax = top._1 * multiToInt
//    settings.lonMax = top._2 * multiToInt + settings.shiftToPostive
//    settings.aggregationLevel = aggregationLevel
//    settings.sizeOfRasterLat = Math.pow(2.toDouble,settings.aggregationLevel.toDouble-1).toInt * 50 //meters
//    settings.sizeOfRasterLon = Math.pow(2.toDouble,settings.aggregationLevel.toDouble-1).toInt * 50 //meters
//    settings.rasterLatLength = ((settings.latMax - settings.latMin) / settings.sizeOfRasterLat).ceil.toInt
//    settings.rasterLonLength = ((settings.lonMax - settings.lonMin) / settings.sizeOfRasterLon).ceil.toInt
//    val raster2 = csv.transformCSVtoTimeRaster()
    val export = new ImportGeoTiff


  }
}
