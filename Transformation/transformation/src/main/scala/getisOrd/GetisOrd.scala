package getisOrd


import geotrellis.macros.{DoubleTileMapper, DoubleTileVisitor, IntTileMapper, IntTileVisitor}
import geotrellis.raster.{ArrayTile, CellType, DoubleArrayTile, DoubleRawArrayTile, IntArrayTile, IntConstantTile, IntRawArrayTile, MutableArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.{Neighborhood, Square}
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import parmeters.Parameters

/**
  * Created by marc on 27.04.17.
  */
class GetisOrd(tile : Tile, cols : Int, rows : Int) {
  var weight : Tile = this.getWeightMatrix(cols, rows) //0,0 for Testing
  var sumOfTile : Double = this.getSummForTile(tile)
  var sumOfWeight : Double = this.getSummForTile(weight)
  var xMean : Double = this.getXMean(tile)
  var powerOfWeight : Double =  getPowerOfTwoForElementsAsSum(weight)
  var powerOfTile : Double =  getPowerOfTwoForElementsAsSum(tile)
  var standardDeviation: Double = this.getStandartDeviationForTile(tile)



  def gStar(layer: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], weight : Array[Int]): Unit ={
    layer.metadata.gridBounds
    layer.count();
    //val tile = IntArrayTile(layer, 9,4)
    Square(1)
  }

  def printG_StarComplete(): Unit ={
    for(i <- 0 to tile.rows-1){
      for(j <- 0 to tile.cols-1){
        print(gStarForTile((i,j)))
        print(";")
      }
      println("")
    }

  }

  def calculateStats(index: (Int, Int)) : Unit = {
    sumOfTile  = getSummForTile(tile)
    xMean  = getXMean(tile)
    powerOfTile  =  getPowerOfTwoForElementsAsSum(tile)
    standardDeviation = getStandartDeviationForTile(tile)
  }

  def getGstartForChildToo(paraParent : Parameters, paraChild : Parameters, childTile : Tile): (Tile, Tile) ={
    createNewWeight(paraParent.weightMatrix)
    val parent = gStarComplete()
    calculateStats(0,0)
    createNewWeight(paraChild.weightMatrix)
    val child = gStarComplete()
    (parent, child)
  }

  def getGstartForChildToo(paraParent : Parameters, paraChild : Parameters): (Tile, Tile) ={
    createNewWeight(paraParent.weightMatrix)
    var parent = gStarComplete()
    val size = (weight.cols,weight.rows)
    createNewWeight(paraChild.weightMatrix)
    if(size._1<weight.cols || size._2<weight.rows){
      throw new IllegalArgumentException("Parent Weight must be greater than Child Weight")
    }
    val child = gStarComplete()
    (parent, child)
  }


  def gStarComplete(): Tile ={
    val tileG = DoubleArrayTile.ofDim(tile.cols, tile.rows)
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        tileG.setDouble(i,j,gStarForTile((i,j)))
      }
    }
    tileG
  }

  def createNewWeight(number : Weight.Value) : Tile = {
    number match {
      case Weight.One => weight = getWeightMatrix(5,5)
      case Weight.Square => weight = getWeightMatrixSquare(3)
      case Weight.Defined => weight = getWeightMatrixDefined(30,30)
      case Weight.Big => weight = getWeightMatrix(50,50)
      case Weight.High => weight = getWeightMatrixHigh()
    }



    sumOfWeight = this.getSummForTile(weight)
    powerOfWeight =  getPowerOfTwoForElementsAsSum(weight)
    weight
  }


  def getNumerator(index: (Int, Int)): Double={
    val xShift = Math.floor(weight.cols/2).toInt
    val yShift = Math.floor(weight.rows/2).toInt
    var sumP1 = 0
    for(i <- 0 to weight.cols-1){
      for(j <- 0 to weight.rows-1){
        if(index._1-xShift+i<0 || index._1-xShift+i>tile.cols-1 || index._2-yShift+j<0 || index._2-yShift+j>tile.rows-1){
          //TODO handle bound Cases
        } else {
          sumP1 += tile.get(index._1-xShift+i, index._2-yShift+j)*weight.get(i,j)
        }

      }
    }
    (sumP1-xMean*sumOfWeight)
  }

  def getDenominator(): Double = {
    (standardDeviation*Math.sqrt((tile.size*powerOfWeight-getSummPowerForWeight())/(tile.size-1)))
  }

  def gStarForTile(index : (Int, Int)) : Double ={
    getNumerator(index)/getDenominator()
  }

  def getStandartDeviationForTile(tile: Tile): Double ={
    val deviation = Math.sqrt(powerOfTile.toFloat/tile.size.toFloat-xMean)
    if(deviation<=0 || deviation==Double.NaN){
      return 1 //TODO handle equal distribution case
    }
    deviation
  }

  def getXMeanSquare(tile: Tile): Double ={
    xMean*xMean
  }

  def getSummForTile(tile: Tile): Double ={
    tile.toArrayDouble().reduce(_+_)
  }

  def getSummPowerForWeight(): Double ={
    sumOfWeight*sumOfWeight
  }

  def getXMean(tile: Tile) : Double ={
    sumOfTile/tile.size
  }

//  def getSummOverTiles(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    layer.map(x=>x._2).reduce(_+_)
//  }

//  def xMean(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    val count = layer.count()
//    getSummOverTiles(layer)/count
//  }

  def getPowerOfTwoForElementsAsSum(tile : Tile): Double ={
    tile.toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
  }

//  def standartDeviation(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    val count = layer.count()
//    layer.map(x=>x._2).fold(IntConstantTile(0, 1, 1))((x, y)=>x+y*y)/count-(xMean(layer)*xMean(layer))
//  }

  def getWeightMatrix(): ArrayTile = {
    //From R example
    val arrayTile = Array[Double](
       0.1, 0.3, 0.5, 0.3, 0.1,
       0.3, 0.8, 1.0, 0.8, 0.3,
       0.5, 1.0, 1.0, 1.0, 0.5,
       0.3, 0.8, 1.0, 0.8, 0.3,
       0.1, 0.3, 0.5, 0.3, 0.1)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }




  def setCenter(array: Array[Array[Double]], cols: Int, rows: Int): Unit = {
    val centerCols =(cols/2.0).toInt
    val centerRows =(rows/2.0).toInt

    for(i <- 1 to 27) {
      for(j <- 1 to 27) {
        array(centerCols+i-13)(centerRows+j-13) = 0.9
      }
    }
    array(centerCols)(centerRows) = 1
  }

  def getWeightMatrixDefined(cols : Int, rows : Int): ArrayTile = {
    var array = Array.ofDim[Double](cols,rows)
    for(i <- 0 to cols-1){
      for(j <- 0 to rows-1){
        array(i)(j) = 0.7
      }
    }
    setCenter(array, cols, rows)



    val weightTile = new DoubleRawArrayTile(array.flatten, cols, rows)
    weightTile
  }

  def getWeightMatrix(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(1)
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }

  def getWeightMatrixSquare(radius : Int): ArrayTile ={
    val arrayTile = Array.ofDim[Int](radius*2+1,radius*2+1)

    for (i <- -radius to radius) {
      for (j <- -radius to radius) {
        if(Math.sqrt(i*i+j*j)<=radius) {
          arrayTile(radius + i)(radius + j) = 1
        } else {
          arrayTile(radius + i)(radius + j) = 0
        }
      }
    }
    val weightTile = new IntRawArrayTile(arrayTile.flatten, radius*2+1,radius*2+1)
    weightTile
  }

  def getWeightMatrixHigh(): ArrayTile ={
    val arrayTile = Array[Double](
      5, 5, 25, 5, 5,
      5, 25, 50.0, 25, 5,
      25, 50, 100, 50, 25,
      5, 25, 50, 25, 5,
      5, 5, 25, 5, 5)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }


}

object Weight extends Enumeration {
  type Weight = Value
  val One, Square, Big, High, Defined = Value
}
