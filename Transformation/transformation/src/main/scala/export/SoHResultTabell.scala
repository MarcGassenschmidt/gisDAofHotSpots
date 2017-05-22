package export

import geotrellis.raster.Tile

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 11.05.17.
  */
class SoHResultTabell {

  def printResults(results : ListBuffer[SoHResult]): Unit ={
    println(header())
    for(r <- results){
      println(r.format())
    }
  }

  def header(): String ={
    "rasterSize(meters),parentFocal,cols,rows,weighParent,weightParentRadius,rasterSizeChild(meters),childFocal,weightChild,weightChildRadius,duration(seconds),downward,upward"
  }
}
