package export

import geotrellis.raster.Tile

/**
  * Created by marc on 11.05.17.
  */
class SoHResultTabell {

  def printResults(results : List[SoHResult]): Unit ={
    println(header())
    for(r <- results){
      println(r.format())
    }
  }

  def header(): String ={
    "rows,cols,weight,weightRows,weightCols,duration,downward,upward"
  }

}
