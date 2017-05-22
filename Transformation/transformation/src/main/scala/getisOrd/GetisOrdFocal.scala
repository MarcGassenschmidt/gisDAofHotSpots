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
//    (RoW-M*sumOfWeight)/(S*((N*powerOfWeight-sumOfWeight*sumOfWeight)/(N-1)).mapDouble(x=> Math.sqrt(x)))
  }

  def setFocalRadius(radius : Double): Unit ={
    F = Circle(radius)
    M = tile.focalMean(F)
    S = tile.focalStandardDeviation(F)
  }

  override def createNewWeight(para : Settings) : Tile = {
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightRadius/2)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightRadius,para.weightRadius)
      case Weight.Big => weight = getWeightMatrix(para.weightRadius,para.weightRadius)
      case Weight.High => weight = getWeightMatrixHigh()
    }
    sumOfWeight = this.getSummForTile(weight)
    powerOfWeight =  getPowerOfTwoForElementsAsSum(weight)
    weight
  }
}
