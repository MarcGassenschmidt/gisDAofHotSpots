package export

import geotrellis.raster.Tile
import getisOrd.Weight.Weight
import parmeters.Parameters

/**
  * Created by marc on 11.05.17.
  */
class SoHResult(parent : Tile, weight : Tile, wParent : Parameters, wChild : Parameters, time : Long, sohValue : (Double,Double)) {
  def format(): String = {
    val parentString  = wParent.sizeOfRasterLat+","+wParent.focal+","+parent.rows+","+parent.cols+","+wParent.weightMatrix+","+wParent.weightCols+","+wParent.weightRows
    val childString = wChild.sizeOfRasterLat+","+wChild.focal+","+wChild.weightMatrix+","+wChild.weightCols+","+wChild.weightRows
    return parentString+","+childString+","+time+","+sohValue._1+","+sohValue._2

  }




}
