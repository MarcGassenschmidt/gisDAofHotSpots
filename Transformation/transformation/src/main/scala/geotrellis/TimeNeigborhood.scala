package geotrellis

import geotrellis.raster.mapalgebra.focal.Square

/**
  * Created by marc on 03.07.17.
  */
trait TimeNeigborhood {
  val a : Int
  val b : Int
  val c : Int
}

case class Spheroid(a:Int,c:Int) extends TimeNeigborhood {
  //https://en.wikipedia.org/wiki/Spheroid
  val b = a

  def isInRange(x:Int,y:Int,z:Int): Boolean = {
    (x*x+y*y)/(a*a)+(z*z)/(c*c)<=1
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
            sum += 1
          }
        }
      }
    }
    sum
  }

}
