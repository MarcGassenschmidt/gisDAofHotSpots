package scripts

import clustering.ClusterHotSpots
import geotrellis.raster.Tile
import getisOrd.{GetisOrd, GetisOrdFocal}
import importExport.ImportGeoTiff
import parmeters.{Scenario, Settings}

/**
  * Created by marc on 19.06.17.
  */
object ExportImagesForPresentation {


  def main(args: Array[String]): Unit = {
    val globalSettings = new Settings
    val geoTiff = new ImportGeoTiff()
    globalSettings.scenario = Scenario.Presentation.toString
    globalSettings.sizeOfRasterLat = 100
    globalSettings.sizeOfRasterLon = 100
    globalSettings.fromFile =true
    var raster : Tile = Generate.getRaster(globalSettings)
    geoTiff.writeGeoTiff(raster,globalSettings,0,0, "Manhatten")

    globalSettings.weightRadius = 20

    writeGStar(globalSettings, geoTiff, raster)
    writeFocalGStar(globalSettings, geoTiff, raster)

  }


  def writeFocalGStar(globalSettings: Settings, geoTiff: ImportGeoTiff, raster: Tile): Unit = {
    globalSettings.focal = true
    globalSettings.focalRange = 40
    val focalGStar = (new GetisOrdFocal(raster, globalSettings)).gStarComplete()
    geoTiff.writeGeoTiff(focalGStar, globalSettings, 0, 0, "focalGStar")
    val focalGStarCluster = (new ClusterHotSpots(focalGStar)).findClusters(1.9, 2)._1
    geoTiff.writeGeoTiff(focalGStarCluster, globalSettings, 0, 0, "focalGStarCluster")
    globalSettings.focalRange = 10
    globalSettings.weightRadius = 2
    val focalGStarBad = (new GetisOrdFocal(raster, globalSettings)).gStarComplete()
    geoTiff.writeGeoTiff(focalGStarBad, globalSettings, 0, 0, "focalGStarBad")
    val focalGStarClusterBad = (new ClusterHotSpots(focalGStarBad)).findClusters(1.9, 2)._1
    geoTiff.writeGeoTiff(focalGStarClusterBad, globalSettings, 0, 0, "focalGStarClusterBad")
  }

  def writeGStar(globalSettings: Settings, geoTiff: ImportGeoTiff, raster: Tile): Unit = {
    globalSettings.focal = false
    val gStar = (new GetisOrd(raster, globalSettings)).gStarComplete()
    geoTiff.writeGeoTiff(gStar, globalSettings, 0, 0, "gStar")
    val gStarCluster = (new ClusterHotSpots(gStar)).findClusters(1.9, 2)._1
    geoTiff.writeGeoTiff(gStarCluster, globalSettings, 0, 0, "gStarCluster")
  }
}
