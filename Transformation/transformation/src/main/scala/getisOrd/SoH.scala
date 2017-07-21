package getisOrd

import clustering.ClusterRelations
import export.TileVisualizer
import geotrellis.{Spheroid, SpheroidHelper}
import geotrellis.raster.{IntRawArrayTile, MultibandTile, Tile}
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable
import scalaz.std.java.enum

/**
  * Created by marc on 09.05.17.
  */
object SoH {

  implicit class TripleAdd(t: (Double, Double, Double)) {
    def +(p: (Double, Double, Double)) = (p._1 + t._1, p._2 + t._2, p._3 + t._3)
  }


  def findNext(b: Int, c: Int, r: Int, mbT: MultibandTile, max : Int) : Double = {
    var distance = max.toDouble
    for(i <- -2 to 2){
      for(j <- -max to max){
        for(k <- -max to max){
          val bandDist = (b+i+24)%24
          val cDist =c+j
          val rDist = r+k
          if(MultibandUtils.isInTile(cDist,rDist,mbT) &&
            mbT.band(bandDist).get(cDist,rDist)!=0){
            distance = Math.sqrt(i*i+c*c+r*r)
          }
        }
     }
    }
    return Math.sqrt(max*max+max*max+2*2)
  }

  def getDistance(parent: MultibandTile, child: MultibandTile) : Double = {
    var res = 0
    var map = new mutable.HashMap[Int,Double]()
    for(b <- 0 to parent.bandCount-1) {
      for (r <- 0 to parent.rows - 1) {
        for (c <- 0 to parent.cols - 1) {
          if(parent.band(b).get(c,r)!=0){
            val nextDistance = findNext(b,c,r,child, 100)
            assert(nextDistance!= -1)
            val key = parent.band(b).get(c,r)
            if(map.contains(key)){
              map.put(key,Math.min(map.get(key).get,nextDistance))
            } else {
              map.put(key,nextDistance)
            }

          }

        }
      }
    }
    val maxDistance = Math.sqrt(100*100+100*100+2*2)
    val sumDist = map.map(x=>x._2).reduce(_+_)
    1-sumDist/(maxDistance*map.size)
  }

  def getMetrikResults(mbT : MultibandTile,
                       mbTWeight : MultibandTile,
                       mbTCluster : MultibandTile,
                       zoomPNCluster : (MultibandTile,MultibandTile),
                       weightPNCluster : (MultibandTile,MultibandTile),
                       focalPNCluster : (MultibandTile,MultibandTile),
                       month : Tile,
                       settings: Settings
                       ): SoHResults ={
    val downUp = getSoHDowAndUp(mbTCluster,weightPNCluster._2)
    //val variance = getVariance(mbTCluster)
    println("deb.1")
    var neighbours = false
    if(focalPNCluster._1==null){
      neighbours =  getSoHNeighbours(mbT,zoomPNCluster,weightPNCluster)
    } else {
      neighbours =  getSoHNeighbours(mbT,zoomPNCluster,weightPNCluster,focalPNCluster)
    }
    println("deb.2")
    val jaccard = getJaccardIndex(mbTCluster,weightPNCluster._2) //Eine Kennzahl
    println("deb.3")
    val percentual = getSDForPercentualTiles(mbTCluster, settings) //Verteilung - Variationskoeffizient
    println("deb.4")
    val time = compareWithTile(mbTCluster,month) //Referenzbild
    println("deb.5")
    val kl = getKL(mbT,mbTWeight) //KL
    println("deb.6")
    val sturcture = measureStructure(mbT) //Struktur
    println("deb.7")
    var distnace = getDistance(weightPNCluster._1,mbTCluster)
    println("deb.8")
    new SoHResults(downUp,neighbours,jaccard,percentual,time,kl,sturcture,distnace)
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
    val meanTotal = MultibandUtils.getHistogramDouble(mbT).mean().get
    s/meanTotal
  }

  def compareWithTile(mbT : MultibandTile, tile : Tile) : (Double,Double) = {
    val sohs = mbT.bands.map(x=>getSoHDowAndUp(x,tile))
    (sohs.map(x=>x._1).reduce(_+_)/sohs.size.toDouble,sohs.map(x=>x._2).reduce(_+_)/sohs.size.toDouble)
  }

  def getKL(parent : MultibandTile, child :MultibandTile): Double ={
    val histo = MultibandUtils.getHistogramInt(parent).merge(MultibandUtils.getHistogramInt(child))
    val max = histo.maxValue().get
    val minPositiv = histo.values().filter(x=>(x)>0).min
    val maxLogValue = Math.max(1.0,Math.log(1/(minPositiv/max.toDouble)))
    val dist = parent.mapBands((f : Int, tile : Tile) => {
      tile.mapDouble((c : Int,r : Int,v : Double)=>{
        var tmp = 0.0
        if(child.band(f).getDouble(c,r)==0 || v==0) tmp = 0.0 else tmp=(v/max)*Math.log(Math.abs((v/max))/Math.abs((child.band(f).getDouble(c,r)/max)))
        tmp
      })
    }).bands.map(x=>x.toArrayDouble().reduce(_+_)).reduce(_+_)/(parent.bandCount*parent.cols*parent.rows)
    Math.abs(dist)
  }

  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile), focalPN : (MultibandTile,MultibandTile)): Boolean ={
    val  downUp = isStable(mbT,zoomPN._2, Neighbours.Aggregation) && isStable(zoomPN._1,mbT, Neighbours.Aggregation) &&
      isStable(mbT,focalPN._2, Neighbours.Focal) && isStable(focalPN._1,mbT, Neighbours.Focal)
      isStable(mbT,weightPN._2, Neighbours.Weight) && isStable(weightPN._1,mbT, Neighbours.Weight)
    return downUp


  }


//To complex
//  val instableIndex = (isStable(mbT,zoomPN._2, Neighbours.Aggregation),isStable(zoomPN._1,mbT, Neighbours.Aggregation),
//    isStable(mbT,focalPN._2, Neighbours.Focal),isStable(focalPN._1,mbT, Neighbours.Focal),
//    isStable(mbT,weightPN._2, Neighbours.Weight),isStable(weightPN._1,mbT, Neighbours.Weight))
//  if(instableIndex==0){
//    return true
//  } else if(instableIndex==-1){
//    return false
//  } else {
//    //One Neigbour is instable
//    return false
//  }


  //  def getInstable(boolean1: Boolean,boolean2: Boolean,boolean3: Boolean,boolean4: Boolean,boolean5: Boolean,boolean6: Boolean,
//                  zoomPN : (MultibandTile,MultibandTile),
//                  weightPN : (MultibandTile,MultibandTile),
//                  focalPN : (MultibandTile,MultibandTile),
//                  getNeighbours :  (Neighbours.Value,MultibandTile) => ((MultibandTile,MultibandTile,Neighbours.Value),(MultibandTile,MultibandTile,Neighbours.Value))): Int ={
//    if(!boolean1 && boolean2 && boolean3 && boolean4 && boolean5 && boolean6){
//      val nb = getNeighbours(Neighbours.Aggregation,zoomPN._1)
//      if(isStable(nb._1._1,zoomPN._1,nb._1._3) && isStable(nb._1._2,zoomPN._1,nb._1._3) &&
//        isStable(nb._2._1,zoomPN._1,nb._2._3) && isStable(nb._2._2,zoomPN._1,nb._2._3)){
//        return 0
//      }
//      return 1
//    } else if(boolean1 && !boolean2 && boolean3 && boolean4 && boolean5 && boolean6){
//      val nb = getNeighbours(Neighbours.Aggregation)
//      if(isStable(nb._1._1,zoomPN._1,nb._1._3) && isStable(nb._1._2,zoomPN._1,nb._1._3) &&
//        isStable(nb._2._1,zoomPN._1,nb._2._3) && isStable(nb._2._2,zoomPN._1,nb._2._3)){
//        return 0
//      }
//      return 2
//    } else if(boolean1 && boolean2 && !boolean3 && boolean4 && boolean5 && boolean6){
//      return 3
//    } else if(boolean1 && boolean2 && boolean3 && !boolean4 && boolean5 && boolean6){
//      return 4
//    } else if(boolean1 && boolean2 && boolean3 && boolean4 && !boolean5 && boolean6){
//      return 5
//    } else if(boolean1 && boolean2 && boolean3 && boolean4 && boolean5 && !boolean6){
//      return 6
//    } else if(boolean1 && boolean2 && boolean3 && boolean4 && boolean5 && boolean6){
//      return 0
//    }
//
//    return -1
//  }
  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile)): Boolean ={
    val  downUp = isStable(mbT,zoomPN._2, Neighbours.Aggregation) && isStable(zoomPN._1,mbT, Neighbours.Aggregation) &&
      isStable(mbT,weightPN._2, Neighbours.Weight) && isStable(weightPN._1,mbT, Neighbours.Weight)
    return downUp
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

  class SoHResults(downUp: (Double, Double), neighbours: Boolean, jaccard: Double, percentual: Double, time: (Double, Double), kl: Double, sturcture: Double, distance : Double){
    override def toString: String = "Metrik results are: \n" +
      "SoH_Down,"+downUp._1+"\n" +
      "SoH_Up,"+downUp._2+"\n" +
      "neighbours,"+neighbours+"\n" +
      "jaccard,"+jaccard+"\n" +
      "percentual,"+percentual+"\n"+
      "time_Down,"+time._1+"\n" +
      "time_Up,"+time._2+"\n" +
      "KL,"+kl+"\n" +
      "structure,"+sturcture+"\n" +
      "distance,"+distance

  }

}

