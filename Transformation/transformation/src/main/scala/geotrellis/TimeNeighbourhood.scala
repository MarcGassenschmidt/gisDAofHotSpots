package geotrellis

import clustering.ClusterHotSpotsTime
import geotrellis.raster.MultibandTile
import geotrellis.raster.mapalgebra.focal.Square
import timeUtils.MultibandUtils

/**
  * Created by marc on 03.07.17.
  */
trait TimeNeighbourhood {
  val a : Int
  val b : Int
  val c : Int
}

case class Spheroid(a:Int,c:Int) extends TimeNeighbourhood {
  //https://en.wikipedia.org/wiki/Spheroid
  val b = a

  def isInRange(x:Double,y:Double,z:Double): Boolean = {
    val tmp1 = (x*x+y*y)/(a*a)
    val tmp2 = (z*z)/(c*c)
    val tmp = tmp1+tmp2
    tmp<=1
  }

  def getSquare(z:Int): Square ={
    assert(z<c)
    //x==y => equation to x
    val r = Math.sqrt(((c*c*a*a)/(z*z))/2).ceil.toInt
    return Square(r)
  }

  def getSum(): Int ={
    var sum = 0
    for(x <- -a to a){
      for(y <- -b to b){
        for(z <- -c to c){
          if(isInRange(x,y,z)){
            //println(x+","+y+","+z)
            sum += 1
          }
        }
      }
    }
    sum
  }


  def countInRange(clusterId: Int, mbT: MultibandTile, startBand: Int, startRows: Int, startCols: Int): (Int,Int,Int,Int) = {
    var sum = 0
    for(x <- -a to a){
      for(y <- -b to b){
        for(z <- -c to c){
          var b = z % 24
          if(z<0){
            z+24
          }
          if(isInRange(x,y,z)
            && MultibandUtils.isInTile(startRows+x,startCols+y,mbT)
            && mbT.band(z).get(startRows+x,startCols+y)==clusterId){
            sum += 1
          }
        }
      }
    }
    (sum,startBand,startRows,startCols)
  }

  def clusterPercent(clusterId : Int, mbT : MultibandTile, startBand : Int, startRows : Int, startCols : Int): Double ={
    var array = new Array[(Int,Int,Int,Int)](27)
    var counter = 0
    for(i <- -1 to 1){
      for(j<- -1 to 1){
        for(k <- -1 to 1){
          array(counter) = countInRange(clusterId,mbT,startBand+i,startRows+j,startCols+k)
          counter+=1
        }
      }
    }
    val max = array.map(x=>x._1).max
    val maxKey = array.filter(x=>x._1==max).head
    if(maxKey._2==startBand && maxKey._3==startRows && maxKey._4==startCols){
      return maxKey._1/SpheroidHelper.getVolume(this)
    }
    clusterPercent(clusterId,mbT,maxKey._2,maxKey._3,maxKey._4)
  }



}



object SpheroidHelper{

  def getSpheroidWithSum(sum : Double, z : Double): Spheroid ={
    //Volume equation
    new Spheroid(Math.sqrt(3/(Math.PI*4)*sum/(z+0.5)).toInt,z.toInt)
  }

  //Nearly same as some for smaller a,c bigger % gap
  def getVolume(spheroid: Spheroid): Double ={
    (4*Math.PI/3)*Math.pow(spheroid.a+0.5,2)*(spheroid.c+0.5)
  }
}