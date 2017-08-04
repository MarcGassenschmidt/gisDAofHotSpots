package getisOrd

import clustering.{ClusterHotSpotsTime, ClusterRelations}
import export.TileVisualizer
import geotrellis.{Spheroid, SpheroidHelper}
import geotrellis.raster.{IntArrayTile, IntRawArrayTile, MultibandTile, Tile}
import osm.ConvertPositionToCoordinate
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

  def getDistance(mbT: MultibandTile) : Double = {
    var counter = 0
    var sum = 0.0
    for(b <- 0 to mbT.bandCount-1) {
      for (r <- 0 to mbT.rows - 1) {
        for (c <- 0 to mbT.cols - 1) {
          if(mbT.band(b).get(c,r)!=0){
            counter += 1
            val ai = dist(mbT,r,c,b)
            val bi = minDist(mbT,r,c,b,5)
            sum += (bi-ai)/Math.max(bi,ai)
          }


        }
      }
    }
    if(counter==0){
      return -1
    }
    sum/counter.toDouble
  }

  private def minDist(mbT : MultibandTile, r : Int, c : Int, b : Int, criticalRange : Int): Double ={
    var visit = MultibandUtils.getEmptyIntMultibandArray(mbT)
    val clusterNumber = mbT.band(b).get(c,r)
    var minDist = Math.sqrt(criticalRange*criticalRange*3)
    for(i <- 0 to criticalRange) {
      for (j <- 0 to criticalRange) {
        for (k <- 0 to criticalRange) {
          val band = b+k
          val row = r+i
          val col = c+j
          if(MultibandUtils.isInTile(band,col,row,mbT) && mbT.band(band).get(col,row)!=0 && mbT.band(band).get(col,row)!=clusterNumber){
            minDist = Math.min(minDist,dist(mbT,row,col,band))
          }
        }
      }
    }
    minDist
  }

  private def dist(mbT : MultibandTile, r : Int, c : Int, b : Int): Double ={
    val visit = MultibandUtils.getEmptyIntMultibandArray(mbT)
    val clusterNumber = mbT.band(b).get(c,r)
    val cluster = getCluster(mbT,r,c,b,clusterNumber,visit,regionQuery(mbT,r,c,b,clusterNumber,visit))
    var sum = 0.0
    for((x,y,z) <- cluster){
      sum += Math.sqrt(Math.pow((x-b),2)+Math.pow((y-c),2)+Math.pow((r-z),2))
    }
    sum/cluster.length.toDouble
  }

  private def getCluster(mbT: MultibandTile, r : Int, c : Int, b : Int, clusterNumber: Int, visit: MultibandTile, neighbourhood : List[(Int,Int,Int)] ) : List[(Int,Int,Int)] = {
    var nextNeigbours = List[(Int,Int,Int)]()
    for((z,x,y) <- neighbourhood){
      nextNeigbours = List.concat(nextNeigbours, regionQuery(mbT, y, x, z, clusterNumber,visit))
    }
    if(nextNeigbours.size>0){
      nextNeigbours = getCluster(mbT, r,c,b, clusterNumber, visit, nextNeigbours)
    }
    return List.concat(nextNeigbours,neighbourhood)
  }

  private def regionQuery(mbT : MultibandTile, r : Int, c : Int, b : Int, clusterNumber : Int,visit : MultibandTile):  List[(Int, Int, Int)] ={
    var neighborhoodVar = List[(Int, Int, Int)]()
    visit.band(b).asInstanceOf[IntRawArrayTile].set(c,r,1)
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        for(k <- -1 to 1) {
          val band = b+k
          val row = r+i
          val col = c+j
          if (MultibandUtils.isInTile(band,col,row, mbT)){
            if(visit.band(band).get(col,row)==0) {
              visit.band(band).asInstanceOf[IntRawArrayTile].set(col, row, 1)
              if (mbT.band(band).get(col, row) == clusterNumber) {
                neighborhoodVar = (band, col, row) :: neighborhoodVar
              }
            }
          }
        }
      }
    }
    neighborhoodVar
  }

  def getPoints(mbT : MultibandTile, settings: Settings): String ={
    val list = new ListBuffer[(Int,Double,Double)]()
    for(b <- 0 to mbT.bandCount-1) {
      for (r <- 0 to mbT.rows - 1) {
        for (c <- 0 to mbT.cols - 1) {
          val tmp = mbT.band(b).getDouble(c,r)
          if(tmp!=0) {
            val cord = ConvertPositionToCoordinate.getGPSCoordinate(r, c, settings)
            list += ((b,cord._1,cord._2))
          }
        }
      }
    }
    var res = "H,Lat,Lon\n"
    list.map(x=>res+=x._1+","+x._2+","+x._3+"\n")
    res
  }

  def getTop100Values(mbT: MultibandTile, settings: Settings) : scala.collection.mutable.Set[(Int,Double,Double,Double)] = {
    //val top100 = MultibandUtils.getHistogramDouble(mbT).values().takeRight(100)
    var set = scala.collection.mutable.Set[(Int,Double,Double,Double)]()
    //var counter = 0
    for(b <- 0 to mbT.bandCount-1) {
      for (r <- 0 to mbT.rows - 1) {
        for (c <- 0 to mbT.cols - 1) {
          val tmp = mbT.band(b).getDouble(c,r)
          if(set.size == 0 || set.minBy(_._4)._4<tmp){
            val cord = ConvertPositionToCoordinate.getGPSCoordinate(r,c,settings)
            if(set.size<100){
              set.add(b,cord._1,cord._2,tmp)
            } else {
              set.remove(set.minBy(_._4))
              set.add(b,cord._1,cord._2,tmp)
            }
          }
        }
      }
    }
    set
  }


//  def getVariogram(mbT : MultibandTile): Unit ={
//    val array = new Array[Double]()
//  }
  def getF1Score(parent : MultibandTile,child : MultibandTile): Double ={
    var xIJ = scala.collection.mutable.HashMap[(Int,Int),Int]()
    var parentCluster = scala.collection.mutable.HashMap[Int,Int]()
    var childCluster = scala.collection.mutable.HashMap[Int,Int]()

    for(b <- 0 to parent.bandCount-1) {
      for (i <- 0 to parent.cols - 1) {
        for (j <- 0 to parent.rows - 1) {
          if (i < child.cols && j < child.rows) {
            if (child.band(b).get(i, j) != 0 && parent.band(b).get(i, j) != 0) {
              val key = (parent.band(b).get(i, j),child.band(b).get(i, j))
              if(xIJ.contains(key)){
                xIJ.put(key,xIJ.get(key).get+1)
              } else {
                xIJ.put(key,1)
              }
            }
            if (parent.band(b).get(i, j) != 0) {
              val key = parent.band(b).get(i, j)
              if(parentCluster.contains(key)){
                parentCluster.put(key,parentCluster.get(key).get+1)
              } else {
                parentCluster.put(key,1)
              }
            }
            if (child.band(b).get(i, j) != 0) {
              val key = child.band(b).get(i, j)
              if(childCluster.contains(key)){
                childCluster.put(key,childCluster.get(key).get+1)
              } else {
                childCluster.put(key,1)
              }
            }
          }
        }
      }
    }
    xIJ.map(x=>{
      val recall = x._2/parentCluster.get(x._1._1).get.toDouble
      val precision = x._2/childCluster.get(x._1._2).get.toDouble
      (2*recall*precision)/(precision+recall)
    } ).reduce(_+_)/(xIJ.size.toDouble)

  }

  def getMoransI(mbT : MultibandTile): Double ={
    val N = mbT.size*mbT.bandCount
    var W = 26
    val mean = MultibandUtils.getHistogramDouble(mbT).mean().get
    var sum = 0.0
    var denominator = 0.0
    for(b <- 0 to mbT.bandCount-1) {
      for (r <- 0 to mbT.rows - 1) {
        for (c <- 0 to mbT.cols - 1) {
          denominator += Math.pow((mbT.band(b).getDouble(c,r) - mean),2)
          for(i <- -1 to 1){
            for(j <- -1 to 1){
              for(k <- -1 to 1){
                if(MultibandUtils.isInTile(b+k,j+c,i+r,mbT) && !(i==0 && j==0 && k==0)){
                  sum += ((mbT.band(b).getDouble(c,r) - mean)*(mbT.band(b+k).getDouble(j+c,i+r) - mean))

                }
              }
            }
          }
        }
      }
    }
    return N/W.toDouble*sum/denominator
  }

  def getMetrikResults(mbT : MultibandTile,
                       mbTCluster : MultibandTile,
                       zoomPNCluster : (MultibandTile,MultibandTile),
                       weightPNCluster : (MultibandTile,MultibandTile),
                       focalPNCluster : (MultibandTile,MultibandTile),
                       month : Tile,
                       settings: Settings,
                       gisCup : MultibandTile): SoHResults ={
    val downUp = getSoHDowAndUp(mbTCluster,weightPNCluster._2)
    //val variance = getVariance(mbTCluster)
    println("deb.1")
    var neighbours = (0,0,0,0,0,0)
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
    val kl = getKL(mbT,weightPNCluster._2) //KL
    println("deb.6")
    val sturcture = -1 //measureStructure(mbT) //Struktur
    println("deb.7")
    var distnace = getDistance(mbTCluster)
    println("deb.8")
    val top100 = getTop100Values(mbT, settings)
    println("deb.9")
    val morans = getMoransI(mbT)
    println("deb.10")
    val f1 = getF1Score(mbTCluster,weightPNCluster._2)
    println("deb.11")
    val ref = (new ClusterRelations()).getPercentualFitting(mbTCluster,gisCup)
    new SoHResults(downUp,neighbours,jaccard,percentual,time,kl,sturcture,distnace,top100,f1,morans,ref)
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
    var down = sohs.map(x=>x._1).reduce(_+_)
    var up = sohs.map(x=>x._2).reduce(_+_)
    if(down!=0){
      down = down/sohs.size.toDouble
    }
    if(up!=0){
      up = up/sohs.size.toDouble
    }
    (down,up)
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
    1-Math.abs(dist)
  }

  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile), focalPN : (MultibandTile,MultibandTile)): (Int,Int,Int,Int,Int,Int) ={
//    val  downUp = isStable(mbT,zoomPN._2, Neighbours.Aggregation) && isStable(zoomPN._1,mbT, Neighbours.Aggregation) &&
//      isStable(mbT,focalPN._2, Neighbours.Focal) && isStable(focalPN._1,mbT, Neighbours.Focal)
//      isStable(mbT,weightPN._2, Neighbours.Weight) && isStable(weightPN._1,mbT, Neighbours.Weight)
//    return downUp

    val  downUp = (isStableI(mbT,zoomPN._2, Neighbours.Aggregation) ,isStableI(zoomPN._1,mbT, Neighbours.Aggregation) ,
      isStableI(mbT,focalPN._2, Neighbours.Focal) , isStableI(focalPN._1,mbT, Neighbours.Focal),
      isStableI(mbT,weightPN._2, Neighbours.Weight) , isStableI(weightPN._1,mbT, Neighbours.Weight))
    return downUp

  }

  def getSoHNeighbours(mbT : MultibandTile, zoomPN : (MultibandTile,MultibandTile), weightPN : (MultibandTile,MultibandTile)): (Int,Int,Int,Int,Int,Int) ={
//    val  downUp = isStable(mbT,zoomPN._2, Neighbours.Aggregation) && isStable(zoomPN._1,mbT, Neighbours.Aggregation) &&
//      isStable(mbT,weightPN._2, Neighbours.Weight) && isStable(weightPN._1,mbT, Neighbours.Weight)
      val  downUp = (isStableI(mbT,zoomPN._2, Neighbours.Aggregation) ,isStableI(zoomPN._1,mbT, Neighbours.Aggregation) ,
                    1,1,
                    isStableI(mbT,weightPN._2, Neighbours.Weight) , isStableI(weightPN._1,mbT, Neighbours.Weight))
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
  def isStableI(child : MultibandTile, parent : MultibandTile, neighbours: Neighbours.Value): Int ={
    if(isStable(child,parent,neighbours)) 1 else 0
  }

  def isStable(child : MultibandTile, parent : MultibandTile, neighbours: Neighbours.Value): Boolean ={
    val tmp = getSoHDowAndUp(child,parent)
    println("neigbours,"+neighbours+","+tmp)
    if(neighbours==Neighbours.Aggregation){
      return tmp>(0.1,0.1)
    } else if(neighbours==Neighbours.Weight){
      return tmp>(0.5,0.5)
    } else if(neighbours==Neighbours.Focal){
      return tmp>(0.6,0.4)
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

  class SoHResults(downUp: (Double, Double), neighbours: (Int,Int,Int,Int,Int,Int), jaccard: Double, percentual: Double,
                   time: (Double, Double), kl: Double, sturcture: Double, distance : Double,
                   top100 : scala.collection.mutable.Set[(Int,Double,Double,Double)], f1score : Double, moransI : Double, gisCup : Double){
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
      "distance,"+distance+"\n"+
      "moransI,"+moransI+"\n"+
      "f1,"+f1score+"\n"+
      "gisCup,"+gisCup+"\n"+
      "top100,"+getTop100Formated
    def getTop100Formated(): String ={
      var res = "xxx,Hx,lati,long,value"
      top100.zipWithIndex.map(x=>res+="\n"+x._2+","+x._1._1+","+x._1._2+","+x._1._3+","+x._1._4)
      res
    }
  }

}

