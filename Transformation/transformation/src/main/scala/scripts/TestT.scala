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
    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "test")
    println(dir)
    val importTer = new ImportGeoTiff()

    //writeBand(settings, dir, importTer)
    val origin = importTer.getMulitGeoTiff(dir+"firstTimeBand.tif",settings)

    settings.layoutTileSize = (origin.cols/4,origin.rows/4)
    val rdd = importTer.repartitionFiles(dir+"firstTimeBand.tif", settings)

    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,path+"raster.tif"))
    //TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    settings.focal = false
    var r = TimeGetisOrd.getGetisOrd(rdd, settings, origin)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(r)
    println("Clustering")
    val hotSpots = clusterHotSpotsTime.findClusters(1.9,5)
    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(origin,settings,path+"hotspots.tif"))
  }

  def writeBand(settings: Settings, dir: String, importTer: ImportGeoTiff): Unit = {
    val transform = new Transformation()
    val mulitBand = transform.transformCSVtoTimeRaster(settings)
    importTer.writeMultiGeoTiff(mulitBand, settings, dir + "firstTimeBand.tif")
  }
}
