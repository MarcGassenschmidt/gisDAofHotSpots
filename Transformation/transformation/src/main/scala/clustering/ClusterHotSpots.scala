package clustering

import geotrellis.raster.{ArrayTile, IntArrayTile, IntRawArrayTile, Tile}

/**
  * Created by marc on 11.05.17.
  */

class ClusterHotSpots(tile : Tile) {


  def replaceNumber(oldNumber: Int, newNumber : Int, clusterTile: IntArrayTile) : Unit = {
    for(i <- 0 to tile.cols-1) {
      for (j <- 0 to tile.rows - 1) {
        if(clusterTile.get(i,j)==oldNumber){
          clusterTile.set(i,j, newNumber)
        }
      }
    }
  }

  private def findRelated(clusterTile: IntArrayTile, clusterCol: Int, clusterRow: Int, range: Double, critical: Double, counterCluster: Int) : Unit = {
    for(i <- -range.toInt to range.toInt){
      for(j <- -range.toInt to range.toInt){
        if(clusterCol+j<tile.cols && clusterCol+j>=0
          && clusterRow+i<tile.rows && clusterRow+i>=0
          && Math.sqrt(j*j+i*i)<=range) {
          if(Math.abs(tile.get(clusterCol+j,clusterRow+i))>critical) {
              clusterTile.set(clusterCol + j, clusterRow + i, counterCluster)
          }
        }
      }
    }
  }

  def regionQuery(range: Double, critical: Double, clusterCol: Int, clusterRow: Int, visit: IntArrayTile) : List[(Int,Int)] = {
    var neighborhood = List[(Int, Int)]()
    for (i <- -range.toInt to range.toInt) {
      for (j <- -range.toInt to range.toInt) {
        if (clusterCol + j < tile.cols && clusterCol + j >= 0
          && clusterRow + i < tile.rows && clusterRow + i >= 0
          && Math.sqrt(j * j + i * i) <= range
          && visit.get(clusterCol + j, clusterRow + i) == 0) {
          visit.set(clusterCol + j, clusterRow + i, 1)
          if (Math.abs(tile.get(clusterCol + j, clusterRow + i)) > critical) {
            neighborhood = (clusterCol + j, clusterRow + i) :: neighborhood
          }
        }
      }
    }
    neighborhood
  }

  def regionQueryNegative(range: Double, critical: Double, clusterCol: Int, clusterRow: Int, visit: IntArrayTile) : List[(Int,Int)] = {
    var neighborhood = List[(Int, Int)]()
    for (i <- -range.toInt to range.toInt) {
      for (j <- -range.toInt to range.toInt) {
        if (clusterCol + j < tile.cols && clusterCol + j >= 0
          && clusterRow + i < tile.rows && clusterRow + i >= 0
          && Math.sqrt(j * j + i * i) <= range
          && visit.get(clusterCol + j, clusterRow + i) == 0) {
          visit.set(clusterCol + j, clusterRow + i, 1)
          if ((tile.get(clusterCol + j, clusterRow + i)) < critical) {
            neighborhood = (clusterCol + j, clusterRow + i) :: neighborhood
          }
        }
      }
    }
    neighborhood
  }

  private def expandCluster(clusterTile: IntArrayTile, range: Double, critical: Double, visit: IntArrayTile, counterCluster: Int, neigbourhoud : List[(Int,Int)] ) : Unit = {
    var nextNeigbours = List[(Int,Int)]()
    for((x,y) <- neigbourhoud){
      clusterTile.set(x,y,counterCluster)
      nextNeigbours = List.concat(nextNeigbours, regionQuery(range, critical, x, y, visit))
    }
    if(nextNeigbours.size>0){
      expandCluster(clusterTile, range, critical, visit, counterCluster, nextNeigbours)
    }
  }

  private def expandClusterNegative(clusterTile: IntArrayTile, range: Double, critical: Double, visit: IntArrayTile, counterCluster: Int, neigbourhoud : List[(Int,Int)] ) : Unit = {
    var nextNeigbours = List[(Int,Int)]()
    for((x,y) <- neigbourhoud){
      clusterTile.set(x,y,counterCluster)
      nextNeigbours = List.concat(nextNeigbours, regionQueryNegative(range, critical, x, y, visit))
    }
    if(nextNeigbours.size>0){
      expandClusterNegative(clusterTile, range, critical, visit, counterCluster, nextNeigbours)
    }
  }

  //inspired by dbscan
  def findClusters(range : Double, critical : Double) : (Tile,Int) ={
    val breaks = tile.histogramDouble.quantileBreaks(100)
    var qNegativ = -critical
    var q = critical
    if(breaks.length==100){
      q = breaks(98)//Math.max(breaks(98),critical)
      qNegativ = breaks(1)//Math.min(breaks(1),-critical)
    }

    var counterCluster = 0

    var tempCluster = 0;
    var visit = IntArrayTile.fill(0,tile.cols,tile.rows)
    var clusterTile = IntArrayTile.fill(0,tile.cols,tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        if((tile.getDouble(i,j))>q) {
          if (clusterTile.get(i, j) == 0) {
            counterCluster += 1
            visit.set(i, j, 1)
            clusterTile.set(i, j, counterCluster)
            expandCluster(clusterTile, range, q, visit, counterCluster, regionQuery(range, q, i, j, visit))
          }
        }
//         else if((tile.getDouble(i,j))<qNegativ){
//          if(clusterTile.get(i,j)==0){
//            counterCluster += 1
//            visit.set(i,j,1)
//            clusterTile.set(i,j,-counterCluster)
//            expandClusterNegative(clusterTile, range, qNegativ, visit, -counterCluster, regionQueryNegative(range, qNegativ, i, j, visit))
//          }
//        }
      }

    }
    (clusterTile,counterCluster)
  }

  //maybe better with dbscan or optics from elki package
  //http://stackoverflow.com/questions/15326505/running-clustering-algorithms-in-elki
  def getClusters(range : Double, critical : Double) : (Tile,Int) = {
    var counterCluster = 0

    var tempCluster = 0;
    var clusterTile = IntArrayTile.fill(0,tile.cols,tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        if(Math.abs(tile.getDouble(i,j))>critical){
          if(clusterTile.get(i,j)==0){
            counterCluster += 1
            findRelated(clusterTile, i, j, range, critical, counterCluster)
          }
        }
      }
    }
    (clusterTile,counterCluster)
  }



}
