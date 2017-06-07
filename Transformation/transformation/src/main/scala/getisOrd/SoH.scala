package getisOrd

import clustering.ClusterRelations
import export.TileVisualizer
import geotrellis.raster.Tile
import parmeters.Settings

/**
  * Created by marc on 09.05.17.
  */
class SoH {
  def getSoHDowAndUp(parent : Tile, child : Tile): (Double, Double) ={
     val sohAll = getSoHDowAndUp((parent,parent.toArray().distinct.length-1), (child,child.toArray().distinct.length-1))
     (sohAll._1,sohAll._2)
  }

  private def getSoHDowAndUp(parent : (Tile,Int), child : (Tile,Int)): (Double, Double, Double, Double) ={
    val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent._1,child._1)
    val childParentInverse = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(child._1,parent._1)
    var down = childParent._2.toDouble/parent._2.toDouble
    var up = 1-childParent._1.toDouble/child._2.toDouble
    var downInv = childParentInverse._2.toDouble/child._2.toDouble
    var upInv = 1-childParentInverse._1.toDouble/parent._2.toDouble
    val visul = new TileVisualizer()
    val t = (new ClusterRelations).rescaleBiggerTile(parent._1,child._1)
    if(t._1.cols!=t._2.cols || t._1.rows!=t._2.rows){
      println("--------------------------------------------------------------------------------------------------------------------------"+t._1.cols+","+t._2.cols +","+ t._1.rows+","+t._2.rows)
    } else {
      visul.visualTileNew(t._1-(t._2), new Settings, "diff")
    }
    if(down<0 || down>1){
      println("----------------------------------------------------------------------------------------------------------------------------")
      println("Down:"+down)
      println("childParent:"+childParent._2)
      println("D:"+parent._2)
      println("Parent"+parent._1.resample(100,100).asciiDrawDouble())
      println("Child"+child._1.resample(100,100).asciiDrawDouble())
      println("D:"+parent._1.toArray().distinct.length)
      println("Details:")
      parent._1.toArray().distinct.map(x => print(x+","))
      throw new IllegalArgumentException("")
      println("----------------------------------------------------------------------------------------------------------------------------")
    }
    if(up<0 || up>1){
      println("----------------------------------------------------------------------------------------------------------------------------")
      println("Up:"+up)
      println("childParent:"+childParent._1)
      println("D:"+child._2)
      println("Parent"+parent._1.resample(100,100).asciiDrawDouble())
      println("Child"+child._1.resample(100,100).asciiDrawDouble())
      println("D:"+child._1.toArray().distinct.length)
      println("Details:")
      child._1.toArray().distinct.map(x => print(x+","))
      throw new IllegalArgumentException("")
      println("----------------------------------------------------------------------------------------------------------------------------")
    }
    (down, up, downInv, upInv)
  }
}
