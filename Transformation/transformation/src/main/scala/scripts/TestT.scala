package scripts

import getisOrd.TimeGetisOrd
import importExport.{ImportGeoTiff, PathFormatter}
import parmeters.Settings
import rasterTransformation.Transformation

/**
  * Created by marc on 03.07.17.
  */
object TestT {
  def main(args: Array[String]): Unit = {
    val settings = new Settings()
    settings.focal = false
    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "test")
    println(dir)
    val importTer = new ImportGeoTiff()

    //writeBand(settings, dir, importTer)

    val rdd = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)
    TimeGetisOrd.getGetisOrd(rdd, settings)
  }

  def writeBand(settings: Settings, dir: String, importTer: ImportGeoTiff): Unit = {
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMulitGeoTiff(mulitBand, settings, dir + "firstTimeBand.tif")
  }
}
