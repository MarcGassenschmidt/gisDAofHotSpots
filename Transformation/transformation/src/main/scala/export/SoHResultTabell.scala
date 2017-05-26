package export

import geotrellis.raster.Tile

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 11.05.17.
  */
class SoHResultTabell {

  def printResults(results : ListBuffer[SoHResult]): Unit ={
    println(results.head.header())
    for(r <- results){
      println(r.format())
    }
  }


}
