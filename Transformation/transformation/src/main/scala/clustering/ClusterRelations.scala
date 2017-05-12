package clustering

import geotrellis.raster.{IntArrayTile, Tile}

/**
  * Created by marc on 12.05.17.
  */
class ClusterRelations {

  def getNumberChildrenAndParentsWhichIntersect(parent : Tile, child : Tile): (Int, Int) ={

    var result = scala.collection.mutable.Set[(Int, Int)]()
    var childSet = scala.collection.mutable.Set[Int]()
    var parentSet = scala.collection.mutable.Set[Int]()
    for(i <- 0 to parent.cols-1){
      for(j <- 0 to parent.rows-1) {
        if(child.get(i,j)!=0 && parent.get(i,j)!=0){
          result += ((child.get(i,j), parent.get(i,j)))
        }

      }
     }
    for((child, parent) <- result){
      childSet += child
      parentSet += parent
    }
    (childSet.size, parentSet.size)
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




//  def getRelations(parent : Tile, child : Tile) : ()
}
