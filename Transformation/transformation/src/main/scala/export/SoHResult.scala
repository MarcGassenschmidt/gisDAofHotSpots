package export

import geotrellis.raster.Tile
import getisOrd.Weight.Weight
import parmeters.Settings

/**
  * Created by marc on 11.05.17.
  */
class SoHResult(parent : Tile, weight : Tile, wParent : Settings, wChild : Settings, time : Long, sohValue : (Double,Double)) {
  def format(): String = {
    val parentString  = wParent.sizeOfRasterLat+","+wParent.focal+","+wParent.focalRange+","+parent.cols+","+parent.rows+","+wParent.weightMatrix+","+wParent.weightRadius
    val childString = wChild.sizeOfRasterLat+","+wChild.focal+","+wChild.weightMatrix+","+wChild.weightRadius
    return parentString+","+childString+","+time+","+sohValue._1+","+sohValue._2

  }

  def header(): String ={
    "rasterSize(meters),parentFocal,focalRange,cols,rows,weighParent,weightParentRadius,rasterSizeChild(meters),childFocal,weightChild,weightChildRadius,duration(seconds),downward,upward"
  }




}
