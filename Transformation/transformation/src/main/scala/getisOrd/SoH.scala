package getisOrd

import clustering.ClusterRelations
import geotrellis.raster.Tile

/**
  * Created by marc on 09.05.17.
  */
class SoH {
    def getSoHDowAndUp(parent : Tile, child : Tile, clusterOfParent : Int, clusterOfChild : Int): (Double, Double) ={
      val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent,child)
      val down = childParent._2.toDouble/clusterOfParent.toDouble
      val up = childParent._1.toDouble/clusterOfChild.toDouble

      (down, up)
    }

  def getSoHDowAndUp(tuple : ((Tile,Int),(Tile,Int))): (Double, Double) ={
    val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(tuple._1._1,tuple._2._1)
    val down = childParent._2.toDouble/tuple._1._2.toDouble
    val up = childParent._1.toDouble/tuple._2._2.toDouble
    val upward = (1-((Math.abs(tuple._2._2-up)).toDouble/Math.abs(tuple._2._2).toDouble))
    //val upward = (1-((Math.abs(tuple._2._2-tuple._1._2)).toDouble/Math.abs(tuple._2._2).toDouble))
    println("Upward compared, (org,frac)"+up+","+upward)
    (down, upward)
  }
}
