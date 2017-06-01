package getisOrd

import clustering.ClusterRelations
import export.TileVisualizer
import geotrellis.raster.Tile
import parmeters.Settings

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

  def getSoHDowAndUp(tuple : ((Tile,Int),(Tile,Int))): (Double, Double, Double, Double) ={
    val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(tuple._1._1,tuple._2._1)
    val childParentInverse = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(tuple._2._1,tuple._1._1)
    var down = childParent._2.toDouble/tuple._1._2.toDouble
    var up = 1-childParent._1.toDouble/tuple._2._2.toDouble
    var downInv = childParentInverse._2.toDouble/tuple._2._2.toDouble
    var upInv = 1-childParentInverse._1.toDouble/tuple._1._2.toDouble
    val visul = new TileVisualizer()
    val t = (new ClusterRelations).rescaleBiggerTile(tuple._1._1,tuple._2._1)
    if(t._1.cols!=t._2.cols || t._1.rows!=t._2.rows){
      println("--------------------------------------------------------------------------------------------------------------------------"+t._1.cols+","+t._2.cols +","+ t._1.rows+","+t._2.rows)
    } else {
      visul.visualTileNew(t._1-(t._2), new Settings, "diff")
    }
    (down, up, downInv, upInv)
  }
}
