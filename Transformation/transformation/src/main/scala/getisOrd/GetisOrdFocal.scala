package getisOrd

import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Circle
import getisOrd.Weight.Weight
import parmeters.Parameters

/**
  * Created by marc on 10.05.17.
  */
class GetisOrdFocal(tile : Tile, cols : Int, rows : Int, focalRadius : Double) extends GetisOrd(tile, cols, rows){
  var focalTile = Circle(focalRadius)
  var focalmean = tile.focalMean(focalTile)
  var focalSD = tile.focalStandardDeviation(focalTile)

  def setFocalRadius(radius : Double): Unit ={
    focalTile = Circle(radius)
    focalmean = tile.focalMean(focalTile)
    focalSD = tile.focalStandardDeviation(focalTile)
  }


  override def createNewWeight(para : Parameters): Tile = {
    para.weightMatrix match {
      case Weight.One => weight = getWeightMatrix(para.weightCols,para.weightRows)
      case Weight.Square => weight = getWeightMatrixSquare(para.weightCols)
      case Weight.Defined => weight = getWeightMatrixDefined(para.weightCols,para.weightRows)
      case Weight.Big => weight = getWeightMatrix(para.weightCols,para.weightRows)
      case Weight.High => weight = getWeightMatrixHigh()
    }
    weight
  }

  override def calculateStats(index: (Int, Int)) : Unit = {
    sumOfTile  = getSummForTile(tile)
    sumOfWeight  = getSummForTile(weight)
    xMean  = getXMean(index)
    powerOfWeight  =  getPowerOfTwoForElementsAsSum(weight)
    //Not needed
    //powerOfTile  =  getPowerOfTwoForElementsAsSum(tile)
    standardDeviation = getStandartDeviationForTile(index)

  }

  override def gStarForTile(index: (Int, Int)): Double = {
    calculateStats(index)
    getNumerator(index)/getDenominator(index)
  }

  def getConvolution(index: (Int, Int)) : Int = {
    val xShift = Math.floor(weight.cols/2).toInt
    val yShift = Math.floor(weight.rows/2).toInt
    var sumP1 = 0
    for(i <- 0 to weight.cols-1) {
      for (j <- 0 to weight.rows - 1) {
        sumP1+=1
      }
    }
    sumP1
  }

  def getDenominator(index: (Int, Int)): Double = {
    val N = getConvolution(index)
    (standardDeviation*Math.sqrt((N*powerOfWeight-getSummPowerForWeight())/(N-1)))
  }

  def getStandartDeviationForTile(index: (Int, Int)): Double = {
    focalSD.getDouble(index._1,index._2)
  }

  def getXMean(index: (Int, Int)): Double = {
    focalmean.getDouble(index._1,index._2)
  }
}
