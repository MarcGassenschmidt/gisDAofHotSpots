package getisOrd

import geotrellis.raster.{DoubleArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.Circle
import getisOrd.Weight.Weight
import org.apache.spark.SparkContext
import parmeters.Settings

/**
  * Created by marc on 10.05.17.
  */
class GetisOrdFocal(tile : Tile, setting : Settings) extends GetisOrd(tile, setting) {
  var F = Circle(setting.focalRange)
  var W = Circle(setting.weightRadius)
  var N = tile.size//tile.focalSum(F)
  var M = tile.focalMean(F)
  var S = tile.focalStandardDeviation(F)
  var RoW = tile.focalSum(W)

  override def gStarComplete(): Tile ={
    (RoW-M*sumOfWeight)/(S*Math.sqrt((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)))
//    // Get current size of heap in bytes
//    val heapSize = Runtime.getRuntime().totalMemory();
//    // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
//    val heapMaxSize = Runtime.getRuntime().maxMemory();
//    // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
//    val heapFreeSize = Runtime.getRuntime().freeMemory();
//
//    Runtime.getRuntime().freeMemory()
//    val d = S*((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)).mapDouble(x => Math.sqrt(x))
//    Runtime.getRuntime().freeMemory()
//    val s = M*sumOfWeight
//    val n = (RoW-s)
//    Runtime.getRuntime().freeMemory()
//    n/(d)
  }

  def setFocalRadius(radius : Double): Unit ={
    F = Circle(radius)
    M = tile.focalMean(F)
    S = tile.focalStandardDeviation(F)
  }

  override def createNewWeight(para : Settings) : Tile = {
    Runtime.getRuntime().freeMemory()
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightRadius)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightRadius,para.weightRadius)
      case Weight.Big => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.High => weight = getWeightMatrixHigh()
    }
    W = Circle(setting.weightRadius)
    RoW = tile.focalSum(W)
    sumOfWeight = this.getSummForTile(weight)
    powerOfWeight =  getPowerOfTwoForElementsAsSum(weight)
    weight
  }

  override def getGstartForChildToo(paraParent : Settings, paraChild : Settings): (Tile, Tile) ={
    createNewWeight(paraParent)
    val parent = gStarComplete()
    val size = (weight.cols,weight.rows)
    createNewWeight(paraChild)
    if(size._1<weight.cols || size._2<weight.rows){
      throw new IllegalArgumentException("Parent Weight must be greater than Child Weight")
    }
    val child = gStarComplete()
    (parent, child)
  }
}
