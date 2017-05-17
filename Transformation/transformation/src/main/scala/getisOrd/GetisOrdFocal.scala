package getisOrd

import geotrellis.raster.{DoubleArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.Circle
import getisOrd.Weight.Weight
import org.apache.spark.SparkContext
import parmeters.Parameters

/**
  * Created by marc on 10.05.17.
  */
class GetisOrdFocal(tile : Tile, cols : Int, rows : Int, focalRadius : Double) extends GetisOrd(tile, cols, rows) {
  var focalTile = Circle(focalRadius)
  var focalmean = tile.focalMean(focalTile)
  var focalSD = tile.focalStandardDeviation(focalTile)

  def gStarForTileSpark(index: (Int, Int), weightF : Tile, tile : Tile, para : Parameters) : Double = {
    val focus = Circle(para.focalRange)
    val mean = tile.focalMean(focus)
    val sd = tile.focalStandardDeviation(focus)
    val powerOfWeightN = weightF.toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
    val sumOfWeightF = weightF.toArrayDouble().reduce(_+_)
    val N = getConvolution(index,weightF)
    val num = getNumerator(index, mean.getDouble(index._1,index._2) , getSummForTile(weightF), weightF)
    val den = getDenominator(index, sd, powerOfWeightN, sumOfWeightF, N)
    num/den
  }

  def getSparkGstart(para : Parameters): Tile = {
    val spark = SparkContext.getOrCreate(para.conf)
    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    val range = 0 to (tile.cols)*(tile.rows)-1
    val weightF = createNewWeight(para)
    val result = spark.parallelize(range).map(index => (index/tile.cols,index%tile.cols,gStarForTileSpark((index/tile.cols,index%tile.cols),weightF,tile, para)))
    for(res <- result.collect()){
      tileG.setDouble(res._1,res._2,res._3)
    }
    tileG

  }

  override def getGstartForChildToo(paraParent : Parameters, paraChild : Parameters): (Tile, Tile) ={
    var parent = getSparkGstart(paraParent)
    val size = (weight.cols,weight.rows)
    if(size._1<weight.cols || size._2<weight.rows){
      throw new IllegalArgumentException("Parent Weight must be greater than Child Weight")
    }
    val child = getSparkGstart(paraChild)
    (parent, child)
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

  def getConvolution(index: (Int, Int), weightF : Tile) : Int = {
    val xShift = Math.floor(weightF.cols/2).toInt
    val yShift = Math.floor(weightF.rows/2).toInt
    var sumP1 = 0
    for(i <- 0 to weightF.cols-1) {
      for (j <- 0 to weightF.rows - 1) {
        sumP1+=1
      }
    }
    sumP1
  }

  def getDenominator(index: (Int, Int)): Double = {
    val N = getConvolution(index)
    (standardDeviation*Math.sqrt((N*powerOfWeight-getSummPowerForWeight())/(N-1)))
  }

  def getDenominator(index: (Int, Int), sd : Tile, powerOfWeightN : Double, sumOfWeightF : Double, N : Double): Double = {
    (sd.getDouble(index._1,index._2)*Math.sqrt((N*powerOfWeightN-sumOfWeightF*sumOfWeightF)/(N-1)))
  }



  def getStandartDeviationForTile(index: (Int, Int)): Double = {
    focalSD.getDouble(index._1,index._2)
  }

  def getXMean(index: (Int, Int)): Double = {
    focalmean.getDouble(index._1,index._2)
  }

 def getNumerator(index: (Int, Int), xMeanF : Double, sumF : Double, weightF: Tile): Double={
    val xShift = Math.floor(weightF.cols/2).toInt
    val yShift = Math.floor(weight.rows/2).toInt
    var sumP1 = 0
    for(i <- 0 to weightF.cols-1){
      for(j <- 0 to weightF.rows-1){
        if(index._1-xShift+i<0 || index._1-xShift+i>tile.cols-1 || index._2-yShift+j<0 || index._2-yShift+j>tile.rows-1){
          //TODO handle bound Cases
        } else {
          sumP1 += tile.get(index._1-xShift+i, index._2-yShift+j)*weightF.get(i,j)
        }

      }
    }
    (sumP1-xMeanF*sumF)
  }

  def setFocalRadius(radius : Double): Unit ={
    focalTile = Circle(radius)
    focalmean = tile.focalMean(focalTile)
    focalSD = tile.focalStandardDeviation(focalTile)
  }
}
