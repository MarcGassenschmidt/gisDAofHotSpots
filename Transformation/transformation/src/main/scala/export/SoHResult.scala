package export

import geotrellis.raster.Tile
import getisOrd.Weight.Weight

/**
  * Created by marc on 11.05.17.
  */
class SoHResult(parent : Tile, weight : Tile, wParent : Weight, wChild : Weight, time : Long, sohValue : (Double,Double)) {
  def format(): String = {
    parent.rows+","+parent.cols+","+wParent+","+wChild+","+time+","+sohValue._1+","+sohValue._2
  }




}
