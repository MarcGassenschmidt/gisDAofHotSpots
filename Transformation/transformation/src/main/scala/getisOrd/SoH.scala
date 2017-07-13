package getisOrd

import clustering.ClusterRelations
import export.TileVisualizer
import geotrellis.{Spheroid, SpheroidHelper}
import geotrellis.raster.{MultibandTile, Tile}
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable
import scalaz.std.java.enum

/**
  * Created by marc on 09.05.17.
  */
object SoH {

  

  def getMetrikResults(mbT : MultibandTile,
                       mbTWeight : MultibandTile,
                       mbTCluster : MultibandTile,
                       zoomPNCluster : (MultibandTile,MultibandTile),
                       weightPNCluster : (MultibandTile,MultibandTile),
                       focalPNCluster : (MultibandTile,MultibandTile),
                       month : Tile,
                       settings: Settings): SoHResults ={
    val downUp = getSoHDowAndUp(mbTCluster,weightPNCluster._2)
    val variance = getVariance(mbTCluster)
    val jaccard = getJaccardIndex(mbTCluster,weightPNCluster._2)
    val percentual = getSDForPercentualTiles(mbTCluster, settings)
    val time = compareWithTile(mbTCluster,month)
    val kl = getKL(mbT,mbTWeight)
    val sturcture = measureStructure(mbT)
    new SoHResults(downUp,variance,jaccard,percentual,time,kl,sturcture)
  }

  def getNumberCluster(tile: MultibandTile) : Int = {
    MultibandUtils.getHistogramInt(tile).values().size-1
  }

  def getSoHDowAndUp(parent : MultibandTile, child : MultibandTile): (Double, Double) ={
    val childParent = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent,child)
    val childParentInverse = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(child,parent)
    val numberClusterParent = getNumberCluster(parent)
    val numberClusterChild = getNumberCluster(child)
    var down = childParent._2.toDouble/numberClusterParent.toDouble
    var up = 1-childParent._1.toDouble/numberClusterChild.toDouble
    (down,up)

  }
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

  def getVariance(mbT : MultibandTile): Unit ={
    var histogramm = MultibandUtils.getHistogramInt(mbT)
    Math.pow(histogramm.statistics().get.stddev,2)
  }

  def getJaccardIndex(parent : MultibandTile, child :MultibandTile): Double ={
    val intersect = (new ClusterRelations()).getNumberChildrenAndParentsWhichIntersect(parent,child)._2
    var histogrammParent = MultibandUtils.getHistogramInt(parent)
    var histogrammChild = MultibandUtils.getHistogramInt(child)
    val union = histogrammChild.merge(histogrammParent).values().length-1
    intersect/union.toDouble
  }

  def getSDForPercentualTiles(mbT : MultibandTile, settings: Settings): Double ={
    val spheroid = new Spheroid(settings.focalRange,settings.focalRangeTime)
    val spheroidSize : Double = spheroid.getSum() //1000 for bigger subsets
    val size : Double= mbT.bandCount*mbT.cols*mbT.rows
    val percent : Double = Math.max(spheroidSize/size,0.01)
    //assert(percent<24/100) or nearly smaller
    val rcSplit = mbT.split(Math.max((mbT.cols/(100*percent)).toInt,1),Math.max((mbT.rows/(100*percent)).toInt,1))
    val brcSplit = rcSplit.map(x=>x.bands).flatten
    val tmp = brcSplit.map(x => x.histogramDouble().values().filter(x=>x!=0))
    val numberClusterInEachSplit = tmp.map(x=>x.size)
    val n : Double = (numberClusterInEachSplit.size-1)
    val mean : Double= numberClusterInEachSplit.reduce(_+_)/n
    val s : Double = Math.sqrt((1/n)*numberClusterInEachSplit.map(x=>Math.pow(x-mean,2)).reduce(_+_))
    s
  }

  def compareWithTile(mbT : MultibandTile, tile : Tile) : (Double,Double) = {
    val sohs = mbT.bands.map(x=>getSoHDowAndUp(x,tile))
    (sohs.map(x=>x._1).reduce(_+_)/sohs.size,sohs.map(x=>x._2).reduce(_+_)/sohs.size)
  }

  def getKL(parent : MultibandTile, child :MultibandTile): Double ={
    val max = MultibandUtils.getHistogramInt(parent).merge(MultibandUtils.getHistogramInt(child)).maxValue().get.toDouble
    parent.mapBands((f : Int, tile : Tile) => {
      tile.mapDouble((c : Int,r : Int,v : Double)=>{
        if(child.band(f).getDouble(c,r)==0 || v==0) 0.0 else (v/max)*Math.log((v/max)/(child.band(f).getDouble(c,r)/max))
      })
    }).bands.map(x=>x.toArrayDouble().reduce(_+_)).reduce(_+_)/(parent.bandCount*parent.cols*parent.rows)
  }

  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile), focalPN : (MultibandTile,MultibandTile)): (Double,Double) ={
      val  downUp = getSoHDowAndUp(mbT,zoomPN._2) + getSoHDowAndUp(zoomPN._1,mbT) +
        getSoHDowAndUp(mbT,focalPN._2) + getSoHDowAndUp(focalPN._1,mbT) +
        getSoHDowAndUp(mbT,weightPN._2) + getSoHDowAndUp(weightPN._1,mbT)
    val tmp = (downUp) / (6.0,6.0)
    return tmp

  }
  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile)): (Double,Double) ={
    val  downUp = getSoHDowAndUp(mbT,zoomPN._2) + getSoHDowAndUp(zoomPN._1,mbT) + getSoHDowAndUp(mbT,weightPN._2) + getSoHDowAndUp(weightPN._1,mbT)
    val tmp = (downUp) / (4.0,4.0)
    return tmp
  }

  def measureStructure(tile : MultibandTile): Double ={
    val values = tile.bands.map(t => t.toArray()).flatten.map(i => i)
    val occurences = values.groupBy(k => k)
    var mean = occurences.map(x=> x._2.size).reduce(_+_)/occurences.size
    val map = new mutable.HashMap[Int,Double]()
    val spheroidArray = new Array[Spheroid](23)
    for(i <- 1 to spheroidArray.length){
      if(mean>1000){
        mean = 1000/(i)
      }
      spheroidArray(i-1) = SpheroidHelper.getSpheroidWithSum(mean,i)
    }
    for(b <- 0 to tile.bandCount-1){
      for(r <- 0 to tile.rows-1){
        for(c <- 0 to tile.cols-1){
          val value = tile.band(b).get(c,r)
          if(value != 0 && !map.contains(value)){
            var maxPercent = 0.0
            for(i <- 0 to spheroidArray.length-1){
                val tmp= spheroidArray(i).clusterPercent(value,tile,b,r,c,0)
                maxPercent = Math.max(tmp,maxPercent)
            }
            map.put(value,maxPercent)
          }
        }
      }
    }

    map.map(x=>x._2).reduce(_+_)/map.size
  }

  def isStable(child : MultibandTile, parent : MultibandTile, neighbours: Neighbours.Value): Boolean ={
    if(neighbours==Neighbours.Aggregation){
      return getSoHDowAndUp(child,parent)>(0.1,0.1)
    } else if(neighbours==Neighbours.Weight){
      return getSoHDowAndUp(child,parent)>(0.5,0.5)
    } else if(neighbours==Neighbours.Focal){
      return getSoHDowAndUp(child,parent)>(0.6,0.4)
    }
    return false
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

  implicit class TuppleAdd(t: (Double, Double)) {
    def +(p: (Double, Double)) = (p._1 + t._1, p._2 + t._2)
    def /(p: (Double, Double)) = (t._1/p._1 ,  t._2/p._2)
    def >(p: (Double, Double)) = (t._1>p._1 &&  t._2>p._2)
  }

  object Neighbours extends Enumeration {
    val Aggregation,Weight,Focal = Value
  }

  class SoHResults(downUp: (Double, Double), variance: Unit, jaccard: Double, percentual: Double, time: (Double, Double), kl: Double, sturcture: Double)

}

