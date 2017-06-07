package clustering


import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{DoubleRawArrayTile, IntArrayTile, IntRawArrayTile, Tile}

/**
  * Created by marc on 12.05.17.
  */
class ClusterRelations {

  def getNumberChildrenAndParentsWhichIntersect(parentTile : Tile, childTile : Tile): (Int, Int) ={
    //val scaled = rescaleBiggerTile(parentTile,childTile)
    assert(parentTile.cols==childTile.cols&& childTile.rows==parentTile.rows)
    val parent = parentTile
    val child = childTile
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


  def sum(): (Int, Int) => Unit = {

    null
  }

  def aggregateTile(tile : Tile): Tile ={

    val result : Tile = tile.downsample(tile.cols/2, tile.rows/2)(f =>
    {var sum = 0
      f.foreach((x:Int,y:Int)=>if(x<tile.cols && y<tile.rows) sum+=tile.get(x,y) else sum+=0)
      sum}
    )
    println(result.asciiDrawDouble())

//    print("T")
    result
  }

  //Greatest common divisor
  //euclide algorithm
  def gcd(a: Int,b: Int): Int = {
    if(b ==0) a else gcd(b, a%b)
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
