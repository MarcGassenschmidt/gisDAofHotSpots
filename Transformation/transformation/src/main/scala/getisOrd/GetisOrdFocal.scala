package getisOrd

import geotrellis.raster.{CellType, DoubleArrayTile, FloatCellType, Tile}
import geotrellis.raster.mapalgebra.focal.{Circle, TargetCell}
import getisOrd.Weight.Weight
import org.apache.spark.SparkContext
import parmeters.Settings

/**
  * Created by marc on 10.05.17.
  */
class GetisOrdFocal(tile: Tile, setting: Settings) extends GetisOrd(tile, setting) {
  var F = Circle(setting.focalRange)
  var W = Circle(setting.weightRadius)
  var N = tile.focalSum(F)
  var M = tile.focalMean(F)
  var S = tile.focalStandardDeviation(F)
  var RoW = tile.focalSum(W)

  def printHeapSize() = {
    // Get current size of heap in bytes
    val heapSize = Runtime.getRuntime().totalMemory();
    // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
    val heapMaxSize = Runtime.getRuntime().maxMemory();
    // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
    val heapFreeSize = Runtime.getRuntime().freeMemory();
    println("Heap Size " + heapSize)
    println("Max Heap Size " + heapMaxSize)
    println("Free Heap Size " + heapFreeSize)
  }

  override def gStarComplete(): Tile = {
    //(RoW-M*sumOfWeight)/(S*((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)).mapIfSetDouble (x => Math.sqrt(x)))
    //    (RoW-M*sumOfWeight)/(S*Math.sqrt((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)))
    println(sumOfWeight)
    println(powerOfWeight)
    val q = S * ((N * powerOfWeight - sumOfWeight * sumOfWeight) / (N - 1)).mapDouble(x => {
      var result: Double = x
      if (x <= 0 || x > Double.MaxValue) {
        result = 1.0
      }
      result
    }).mapDouble(x => Math.sqrt(Math.max(0, x)))
    val s = M * sumOfWeight
    val n = (RoW - s)
    println(q.resample(100, 100).asciiDrawDouble())
    //printHeapSize()
    println(n.resample(100, 100).asciiDrawDouble())

    n.mapDouble((x, y, v) =>
      if (v==Double.NaN
        || q.getDouble(x, y) == 0
        || q.getDouble(x, y) == Double.MinValue
        || q.getDouble(x, y) == Double.PositiveInfinity
        || q.getDouble(x, y) == Double.NaN
        || q.getDouble(x, y) == Double.NegativeInfinity)
        0.0
      else
        x / q.getDouble(x, y)
    )
  }


  def setFocalRadius(radius: Double): Unit = {
    F = Circle(radius)
    M = tile.focalMean(F)
    S = tile.focalStandardDeviation(F)
  }

  override def createNewWeight(para: Settings): Tile = {
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightRadius, para.weightRadius)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightRadius)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightRadius, para.weightRadius)
      case Weight.Big => weight = getWeightMatrix(para.weightRadius, para.weightRadius)
      case Weight.High => weight = getWeightMatrixHigh()
    }
    W = Circle(setting.weightRadius)
    RoW = tile.focalSum(W)
    sumOfWeight = this.getSummForTile(weight)
    powerOfWeight = getPowerOfTwoForElementsAsSum(weight)
    weight
  }

  override def getGstartForChildToo(paraParent: Settings, paraChild: Settings): (Tile, Tile) = {
    createNewWeight(paraParent)
    val parent = gStarComplete()
    val size = (weight.cols, weight.rows)
    createNewWeight(paraChild)
    if (size._1 < weight.cols || size._2 < weight.rows) {
      throw new IllegalArgumentException("Parent Weight must be greater than Child Weight")
    }
    val child = gStarComplete()
    (parent, child)
  }
}
