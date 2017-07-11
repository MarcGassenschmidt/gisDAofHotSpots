package clustering


import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{DoubleRawArrayTile, IntArrayTile, IntRawArrayTile, MultibandTile, Tile}

/**
  * Created by marc on 12.05.17.
  */
class ClusterRelations {

  def getNumberChildrenAndParentsWhichIntersect(parentTile : Tile, childTile : Tile): (Int, Int) ={
    val scaled = rescaleBiggerTile(parentTile,childTile)
    //assert(parentTile.cols==childTile.cols&& childTile.rows==parentTile.rows)
    val parent = scaled._1
    val child = scaled._2
    var result = scala.collection.mutable.Set[(Int, Int)]()
    var childSet = scala.collection.mutable.Set[Int]()
    var parentSet = scala.collection.mutable.Set[Int]()
    //assert(parent.cols==child.cols && parent.rows==child.rows)
    for(i <- 0 to parent.cols-1){
      for(j <- 0 to parent.rows-1) {
        if (i < child.cols && j < child.rows) {
          if (child.get(i, j) != 0 && parent.get(i, j) != 0) {
            result += ((child.get(i, j), parent.get(i, j)))
          }
          if (parent.get(i, j) == 0 && child.get(i, j) != 0) {
            childSet += child.get(i, j)
          }
        }
      }
     }
    for((child, parent) <- result){
      //childSet += child
      parentSet += parent
    }
    (childSet.size, parentSet.size)
  }

  def getNumberChildrenAndParentsWhichIntersect(parentTile : MultibandTile, childTile : MultibandTile): (Int, Int) ={
    val scaled = rescaleBiggerTile(parentTile,childTile)

    val parent = scaled._1
    val child = scaled._2
    var result = scala.collection.mutable.Set[(Int, Int)]()
    var childSet = scala.collection.mutable.Set[Int]()
    var parentSet = scala.collection.mutable.Set[Int]()
    //assert(parent.cols==child.cols && parent.rows==child.rows)
    for(b <- 0 to parent.bandCount-1) {
      for (i <- 0 to parent.cols - 1) {
        for (j <- 0 to parent.rows - 1) {
          if (i < child.cols && j < child.rows) {
            if (child.band(b).get(i, j) != 0 && parent.band(b).get(i, j) != 0) {
              result += ((child.band(b).get(i, j), parent.band(b).get(i, j)))
            }
            if (parent.band(b).get(i, j) == 0 && child.band(b).get(i, j) != 0) {
              childSet += child.band(b).get(i, j)
            }
          }
        }
      }
    }
    for((child, parent) <- result){
      //childSet += child
      parentSet += parent
    }
    //Schnittmenge - Intersect //Anzahl der Parent in der Schnittmenge
    (childSet.size, parentSet.size)
  }

  def rescaleBiggerTile(parent : MultibandTile, child : MultibandTile): (MultibandTile,MultibandTile) ={
    if(parent.rows>child.rows){
      if(parent.cols>child.cols){
        return (parent,child.resample(parent.cols, parent.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols,parent.rows), child.resample(child.cols, parent.rows))
      }
    } else if(parent.rows<child.rows) {
      if(parent.cols>child.cols){
        return (parent.resample(parent.cols,child.rows), child.resample(parent.cols,child.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols, child.rows), child)
      }
    }
    return (parent, child)
  }





  def rescaleBiggerTile(parent : Tile, child : Tile): (Tile,Tile) ={
    if(parent.rows>child.rows){
      if(parent.cols>child.cols){
        return (parent,child.resample(parent.cols, parent.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols,parent.rows), child.resample(child.cols, parent.rows))
      }
    } else if(parent.rows<child.rows) {
      if(parent.cols>child.cols){
        return (parent.resample(parent.cols,child.rows), child.resample(parent.cols,child.rows))
      } else if(parent.cols<child.cols){
        return (parent.resample(child.cols, child.rows), child)
      }
    }
    return (parent, child)
  }

}
