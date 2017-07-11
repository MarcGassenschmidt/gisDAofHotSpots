package getisOrd

import clustering.ClusterRelations
import export.TileVisualizer
import geotrellis.{Spheroid, SpheroidHelper}
import geotrellis.raster.{MultibandTile, Tile}
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable

/**
  * Created by marc on 09.05.17.
  */
object SoH {
  def getSoHDowAndUp(parent : Tile, child : Tile): (Double, Double) ={
     val sohAll = getSoHDowAndUp((parent,parent.toArray().distinct.length-1), (child,child.toArray().distinct.length-1))
     var result = (sohAll._1,sohAll._2)
    if(result._2==Double.NaN && result._1==Double.NaN){
      return (0,0)
    }
    if(result._1==Double.NaN){
      return (0,result._2)
    }
    if(result._2==Double.NaN){
      return (result._1,0)
    }
    return result
  }

  def getJaccardIndex(parent : MultibandTile, child :MultibandTile): Double ={
    val intersect = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent,child)._1
    var histogrammParent = MultibandUtils.getHistogramInt(parent)
    var histogrammChild = MultibandUtils.getHistogramInt(child)
    val union = histogrammChild.merge(histogrammParent).totalCount()
    intersect/union.toDouble
  }

  def measureStructure(tile : MultibandTile): Double ={
    val values = tile.bands.map(t => t.toArray()).flatten.map(i => i)
    val occurences = values.groupBy(k => k)
    val mean = occurences.map(x=> x._2.size).reduce(_+_)/occurences.size
    val map = new mutable.HashMap[Int,Double]()
    val spheroidArray = new Array[Spheroid](24)
    for(i <- 0 to spheroidArray.length-1){
      spheroidArray(i) = SpheroidHelper.getSpheroidWithSum(mean,i)
    }
    for(b <- 0 to tile.bandCount-1){
      for(r <- 0 to tile.rows-1){
        for(c <- 0 to tile.cols-1){
          val value = tile.band(b).get(c,r)
          if(value != 0 && !map.contains(value)){
            var maxPercent = 0.0
            for(i <- 0 to spheroidArray.length-1){
                maxPercent = Math.max(spheroidArray(i).clusterPercent(value,tile,b,r,c),maxPercent)
            }
            map.put(value,maxPercent)
          }
        }
      }
    }
    map.map(x=>x._2).reduce(_+_)/map.size
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
