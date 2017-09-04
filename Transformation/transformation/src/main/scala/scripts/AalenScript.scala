package scripts

import clustering.ClusterHotSpots
import getisOrd.{GetisOrd, GetisOrdFocal}
import importExport.{ImportGeoTiff, TifType}
import parmeters.Settings
import rasterTransformation.Transformation

/**
  * Created by marc on 04.09.17.
  */
object AalenScript {
  def main(args: Array[String]): Unit = {
    val csv = new Transformation
    val raster = csv.transformCSVPolygon()
    val export = new ImportGeoTiff

    val settings = new Settings()
    settings.scenario = "Special"
    settings.focalRange = 50
    settings.weightRadius = 10

    settings.focal = false
    export.writeGeoTiff(raster,settings,TifType.Raw)
    val gStar = new GetisOrd(raster,settings)
    val gStarR = gStar.gStarComplete()
    export.writeGeoTiff(gStarR,settings,TifType.GStar)
    val clustering = new ClusterHotSpots(gStarR)
    export.writeGeoTiff(clustering.findClustersTest(),settings,TifType.Cluster)

    settings.focal = true
    val gStarFocal = new GetisOrdFocal(raster,settings)
    val gStarFocalR = gStarFocal.gStarDoubleComplete()
    export.writeGeoTiff(gStarFocalR,settings,TifType.GStar)
    val clustering2 = new ClusterHotSpots(gStarFocalR)
    export.writeGeoTiff(clustering2.findClustersTest(),settings,TifType.Cluster)
  }
}
