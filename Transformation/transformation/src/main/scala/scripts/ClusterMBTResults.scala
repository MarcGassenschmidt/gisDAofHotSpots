package scripts

import clustering.ClusterHotSpotsTime
import geotrellis.raster.Tile
import getisOrd.TimeGetisOrd
import importExport.{ImportGeoTiff, PathFormatter}
import parmeters.Settings
import rasterTransformation.Transformation

/**
  * Created by marc on 19.07.17.
  */
object ClusterMBTResults {
  def main(args: Array[String]): Unit = {
    val settings = new Settings()
    settings.focal = false

    val path = new PathFormatter()
    val dir = path.getDirectory(settings, "MetrikValidations")
    println(dir)
    val importTer = new ImportGeoTiff()


    val gStar = importTer.getMulitGeoTiff(dir+"gStar.tif",settings)
    var clusterHotSpotsTime = new ClusterHotSpotsTime(gStar)
    val hotSpots = clusterHotSpotsTime.findClusters(1.9,5)
    //println(hotSpots._1.band(10).resample(50,50).asciiDraw())
    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(hotSpots._1,settings,dir+"ClustergStar.tif"))
    (new ImportGeoTiff().writeMultiGeoTiff(hotSpots._1,settings,dir+"ClustergStar.tif"))

    settings.focal = true
    val dirF = path.getDirectory(settings, "test")
    println(dirF)
    val focalgStar = importTer.getMulitGeoTiff(dirF+"focalgStar.tif",settings)
    var clusterHotSpotsTimeF = new ClusterHotSpotsTime(focalgStar)
    val hotSpotsF = clusterHotSpotsTime.findClusters(1.9,5)
    //println(hotSpotsF._1.band(10).resample(50,50).asciiDraw())

    (new ImportGeoTiff().writeMultiGeoTiff(hotSpotsF._1,settings,dirF+"ClustergStar.tif"))
    //(new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(hotSpotsF._1,settings,dirF+"ClustergStar.tif"))
  }



}
