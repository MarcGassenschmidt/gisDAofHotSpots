package clustering

import geotrellis.raster.{IntArrayTile, Tile}

/**
  * Created by marc on 12.05.17.
  */
class ClusterRelations {

  def getNumberChildrenAndParentsWhichIntersect(parent : Tile, child : Tile): (Int, Int) ={
    //TODO guards an rescale
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




//  def getRelations(parent : Tile, child : Tile) : ()
}
