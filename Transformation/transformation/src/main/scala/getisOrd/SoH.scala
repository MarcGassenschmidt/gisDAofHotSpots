package getisOrd

import clustering.ClusterRelations
import geotrellis.raster.Tile

/**
  * Created by marc on 09.05.17.
  */
class SoH {
    def getSoHDowAndUp(parent : Tile, child : Tile, clusterOfParent : Int, clusterOfChild : Int): (Double, Double) ={
      val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent,child)
      val down = childParent._2/clusterOfParent
      val up = childParent._1/clusterOfChild
      (down, up)
    }
}
