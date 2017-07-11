package timeUtils

import geotrellis.raster.MultibandTile
import geotrellis.raster.histogram.Histogram

/**
  * Created by marc on 11.07.17.
  */
object MultibandUtils {

  def getHistogramDouble(mbT : MultibandTile): Histogram[Double] ={
    var histogram = mbT.band(0).histogramDouble()
    for(b <- 1 to mbT.bandCount-1){
      histogram = histogram.merge(mbT.band(b).histogramDouble())
    }
    histogram
  }

  def getHistogramInt(mbT : MultibandTile): Histogram[Int] ={
    var histogram = mbT.band(0).histogram
    for(b <- 1 to mbT.bandCount-1){
      histogram = histogram.merge(mbT.band(b).histogram)
    }
    histogram
  }

  def isInTile(x: Int, y: Int, mbT: MultibandTile): Boolean = {
    x>=0 && y>=0 && x<mbT.cols && y < mbT.rows
  }

}


