package scripts

import clustering.ClusterHotSpots
import export.SoHResult
import geotrellis.raster.{DoubleRawArrayTile, IntRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import getisOrd.SoH.SoHR
import getisOrd.{GetisOrd, GetisOrdFocal, TimeGetisOrd}
import importExport.{ImportGeoTiff, TifType}
import org.apache.commons.collections.map.HashedMap
import parmeters.Settings
import rasterTransformation.Transformation
import scenarios.{DifferentRatio, GenericScenario}
import timeUtils.MultibandUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 04.09.17.
  */
object AalenScript {

  def aggregateToZoom(tile: Tile, i: Int): Tile = {
    val empty = new DoubleRawArrayTile(Array.fill(tile.size/i)(0), tile.cols/i, tile.rows/i)
    val count = new mutable.HashMap[(Int,Int), Int]()
    var counter = 0
    val mean = tile.histogramDouble().mean()
    for(r <- 0 to tile.rows-1){
      for(c <- 0 to tile.cols-1){
        val indexC =(c / i.toDouble).round.toInt
        val indexR = (r / i.toDouble).round.toInt
        if(TimeGetisOrd.isNotNaN(tile.getDouble(c,r)) && tile.getDouble(c,r)!=0) {
          empty.setDouble(indexC,indexR, empty.getDouble(indexC,indexR) + tile.getDouble(c, r) + 0.0)
          if(!count.contains(indexC,indexR)){
            count.put((indexC,indexR),1)
          } else {
            count.update((indexC,indexR),count.get(indexC,indexR).get+1)
          }

        } else {
          empty.setDouble(indexC,indexR, 0)
        }

      }
    }
    for(c <- count){
      empty.setDouble(c._1._1,c._1._2,empty.getDouble(c._1._1,c._1._2)/c._2.toDouble)
    }
    empty//.map(x=>if(x<0 || x>1) 0 else x)
  }

  def main(args: Array[String]): Unit = {
    val csv = new Transformation
//    val raster = csv.transformCSVPolygon()
    val export = new ImportGeoTiff
    var raster = export.readGeoTiff("/home/marc/media/SS_17/output/Output.tif")
    //raster = export.readGeoTiff("/home/marc/media/SS_17/output/GIS_Daten/Mulitbandfalse/2016/1/Raster/a2_.tif")
    //raster = raster.mapDouble(x=>x*10)
    val settings = new Settings()
    settings.scenario = "Special"
    settings.focalRange = 50
    settings.weightRadius = 30
    settings.focal = false
    raster = aggregateToZoom(raster, 4)
    export.writeGeoTiff(raster,settings,TifType.Raw)
    var tmp = raster.toArrayDouble().reduce(_+_)
    println(raster.histogramDouble().toString)
    println(raster.histogramDouble().mean())
    println(raster.histogramDouble().minMaxValues())
//    raster = aggregateToZoom(raster,2)
//    tmp = raster.toArrayDouble().reduce(_+_)
//    export.writeGeoTiff(new IntRawArrayTile(raster.toArrayDouble().map(x=>x.toInt),raster.cols,raster.rows),settings,TifType.Raw)
//    println(raster.histogramDouble().toString)
//    println(raster.histogramDouble().mean())
//    println(raster.histogramDouble().minMaxValues())
//    raster.histogramDouble().values().map(x=>println(x))
   val gStar = new GetisOrd(raster,settings)
   val gStarR = gStar.gStarDoubleComplete()
//    gStarR.histogramDouble().values().map(x=>println(x))
    export.writeGeoTiff(gStarR,settings,TifType.GStar)
    val clustering = new ClusterHotSpots(gStarR)
    export.writeGeoTiff(clustering.findClustersTest(),settings,TifType.Cluster)



   // raster = gen.aggregateTile(raster)
    settings.focal = true
    val gStarFocal = new GetisOrdFocal(raster,settings)
    var gStarFocalR = gStarFocal.gStarComplete()
    println(gStarFocalR.histogramDouble().minMaxValues())
    gStarFocalR.histogramDouble().values().map(x=>println(x))

    export.writeGeoTiff(gStarFocalR,settings,TifType.GStar)
    val clustering2 = new ClusterHotSpots(gStarFocalR)
    export.writeGeoTiff(clustering2.findClustersTest(),settings,TifType.Cluster)
  }


}
