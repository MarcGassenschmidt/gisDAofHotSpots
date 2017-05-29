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
    var F = Circle(setting.focalRange)
    var W = Circle(setting.weightRadius) 
    var N = tile.focalSum(F)

    var S = tile.focalStandardDeviation(F)

    val q = S * ((N * powerOfWeight - sumOfWeight * sumOfWeight) / (N - 1)).mapDouble(x => Math.sqrt(Math.max(0, x)))
    S = null
    N = null
    var M = tile.focalMean(F)
    M = M * sumOfWeight

    var RoW = tile.focalSum(W) //Todo if different Weight then 1
    RoW = (RoW - M)
    M = null
  //  println(q.resample(100, 100).asciiDrawDouble())
  //  printHeapSize()
//    println(n.resample(100, 100).asciiDrawDouble())


    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        val qt = q.getDouble(i,j)
        if(qt<=0){
          tileG.setDouble(i,j,0)
        } else {
          tileG.setDouble(i,j,RoW.getDouble(i,j)/q.getDouble(i,j))
        }
      }
    }
    tileG

  }

  def debugFocalgStar(): (Tile,Tile,Tile,Tile,Tile,Tile,Tile) = {
    //(RoW-M*sumOfWeight)/(S*((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)).mapIfSetDouble (x => Math.sqrt(x)))
    //    (RoW-M*sumOfWeight)/(S*Math.sqrt((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)))
    var F = Circle(setting.focalRange)
    var W = Circle(setting.weightRadius)
    var N = tile.focalSum(F)

    var S = tile.focalStandardDeviation(F)

    val q = S * ((N * powerOfWeight - sumOfWeight * sumOfWeight) / (N - 1)).mapDouble(x => Math.sqrt(Math.max(0, x)))
    var M = tile.focalMean(F)
    val MW = M * sumOfWeight

    var RoW = tile.focalSum(W) //Todo if different Weight then 1
    val RoWM = (RoW - MW)

    //  println(q.resample(100, 100).asciiDrawDouble())
    //  printHeapSize()
    //    println(n.resample(100, 100).asciiDrawDouble())


    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        val qt = q.getDouble(i,j)
        if(qt.equals(Double.NaN) || qt.equals(Double.MinValue) || qt.equals(Double.NegativeInfinity) || qt<=0){
          tileG.setDouble(i,j,0)
        } else {
          tileG.setDouble(i,j,RoWM.getDouble(i,j)/q.getDouble(i,j))
          println(RoWM.getDouble(i,j)/q.getDouble(i,j))
        }
      }
    }
    (tileG,RoWM,S,q,N,RoW,MW)

  }




  override def createNewWeight(para: Settings): Tile = {
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightRadius, para.weightRadius)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightRadius)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightRadius, para.weightRadius)
      case Weight.Big => weight = getWeightMatrix(para.weightRadius, para.weightRadius)
      case Weight.High => weight = getWeightMatrixHigh()
      case Weight.Sigmoid => weight = getWeightMatrixSquareSigmoid(para.weightRadius, para.weightRadius/2)

    }

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
