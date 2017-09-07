package scripts

import clustering.ClusterRelations
import geotrellis.raster.IntRawArrayTile
import getisOrd.SoH
import importExport.{ImportGeoTiff, PathFormatter, TifType}
import parmeters.Scenario
import rasterTransformation.Transformation
import timeUtils.MultibandUtils

/**
  * Created by marc on 05.09.17.
  */
object ZoomScript {
  def main(args: Array[String]): Unit = {
    val export = new ImportGeoTiff
    val settings = MetrikValidation.defaultSetting()
    var validate = new ClusterRelations()

    settings.focal = true
    settings.zoomlevel = 1
    val f1 = export.getMulitGeoTiff(settings,TifType.Cluster)
    settings.zoomlevel = 2
    val f2 = export.getMulitGeoTiff(settings,TifType.Cluster)

    val flessRows = f2.rows-f1.rows
    val flessCols = f2.cols-f1.cols

    val fpart = MultibandUtils.getEmptyIntMultibandArray(f1)
    for(j<- 0 to f1.rows-1){
      for(i <- 0 to f1.cols-1){
        for(k <- 0 to f1.bandCount-1){
          fpart.band(k).asInstanceOf[IntRawArrayTile].set(i,j,f2.band(k).get(flessCols/2+i,flessRows/2+j))
        }
      }
    }

    println("Focal"+validate.getPercentualFitting(fpart,f1))

    settings.focal = false
    settings.zoomlevel = 1
    val g1 = export.getMulitGeoTiff(settings,TifType.Cluster)
    settings.zoomlevel = 2
    val g2 = export.getMulitGeoTiff(settings,TifType.Cluster)

    val glessRows = g2.rows-g1.rows
    val glessCols = g2.cols-g1.cols
    val part = MultibandUtils.getEmptyIntMultibandArray(g1)
    for(j<- 0 to g1.rows-1){
      for(i <- 0 to g1.cols-1){
        for(k <- 0 to g1.bandCount-1){
          part.band(k).asInstanceOf[IntRawArrayTile].set(i,j,g2.band(k).get(glessCols/2+i,glessRows/2+j))
        }
      }
    }

    println("GStar"+validate.getPercentualFitting(part,g1))
  }
}
