package scripts

import clustering.ClusterHotSpotsTime
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

    val dir = PathFormatter.getDirectory(settings, "test")
    println(dir)
    val importTer = new ImportGeoTiff()

    writeBand(settings, dir, importTer)
    val origin = importTer.getMulitGeoTiff(dir+"firstTimeBand.tif")
    assert(origin.cols % 4==0 && origin.rows % 4==0)
    settings.layoutTileSize = ((origin.cols/4.0).floor.toInt,(origin.rows/4.0).floor.toInt)
    val rdd = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)

    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"raster.tif"))

    val r1 = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    println(dir+"gStar.tif")
    importTer.writeMultiGeoTiff(r1,settings,dir+"gStar.tif")



    settings.focal = true
    val dirF = PathFormatter.getDirectory(settings, "test")
    println(dirF+"focalgStar.tif")
    var r2 = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    importTer.writeMultiGeoTiff(r2,settings,dirF+"focalgStar.tif")
//    var clusterHotSpotsTime = new ClusterHotSpotsTime(r)
//    println("Clustering")
//    val hotSpots = clusterHotSpotsTime.findClusters(1.9,5)
//    println(dir+"hotspots.tif")
//    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,dir+"hotspots.tif"))
  }

  def writeBand(settings: Settings, dir: String, importTer: ImportGeoTiff): Unit = {
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, dir + "firstTimeBand.tif")
  }
}
