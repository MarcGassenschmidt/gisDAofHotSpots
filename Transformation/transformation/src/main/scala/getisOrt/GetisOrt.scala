package gisOrt

import geotrellis.raster.{IntArrayTile, IntConstantTile, Tile}
import geotrellis.raster.mapalgebra.focal.{Neighborhood, Square}
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD

/**
  * Created by marc on 27.04.17.
  */
class GetisOrt {

  def gStar(layer: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], weight : Array[Int]): Unit ={
    layer.metadata.gridBounds

    layer.count();
    //val tile = IntArrayTile(layer, 9,4)
    Square(1)

  }

  def getNumerator(tile: Tile, weight: Tile, index: (Int, Int)): Double={
    val xShift = Math.floor(weight.rows/2).toInt
    val yShift = Math.floor(weight.cols/2).toInt
    var sumP1 = 0
    for(i <- 1 to weight.rows){
      for(j <- 1 to weight.cols){
        if(index._1-xShift+i<0 || index._1-xShift+i>weight.rows || index._2-yShift+j<0 || index._2-yShift+j>weight.cols){
          //TODO handle bound Cases
        } else {
          sumP1 += tile.get(index._1-xShift+i, index._2-yShift+j)*weight.get(i,j)
        }

      }
    }
    (sumP1-getXMean(tile)*getSummForTile(weight))
  }

  def getDenmonitor(tile: Tile, weight: Tile): Double = {
    (getStandartDeviationForTile(tile)*Math.sqrt((getPowerOfTwoForElementsAsSum(weight)-getSummForTile(weight)*getSummForTile(weight))/(tile.size-1)))
  }

  def gStarForTile(tile : Tile, index : (Int, Int), weight: Tile) : Double ={
    getNumerator(tile, weight, index)/getDenmonitor(tile, weight)
  }

  def getStandartDeviationForTile(tile: Tile): Double ={
    Math.sqrt(getPowerOfTwoForElementsAsSum(tile)/tile.size-getXMeanSquare(tile))
  }

  def getXMeanSquare(tile: Tile): Int ={
    getXMean(tile)*getXMean(tile)
  }

  def getSummForTile(tile: Tile): Int ={
    tile.toArray().reduce(_+_)
  }

  def getXMean(tile: Tile) : Int ={
    getSummForTile(tile)/tile.size
  }

//  def getSummOverTiles(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    layer.map(x=>x._2).reduce(_+_)
//  }

//  def xMean(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    val count = layer.count()
//    getSummOverTiles(layer)/count
//  }

  def getPowerOfTwoForElementsAsSum(tile : Tile): Int ={
    tile.toArray().foldLeft(0)((x,y)=>x+y*y)
  }

//  def standartDeviation(layer: RDD[(SpaceTimeKey, Tile)]): Tile ={
//    val count = layer.count()
//    layer.map(x=>x._2).fold(IntConstantTile(0, 1, 1))((x, y)=>x+y*y)/count-(xMean(layer)*xMean(layer))
//  }

  def getWeightMatrix(): Array[Byte] = {
    //From R example
    val arrayTile = Array[Double](
       0.1, 0.3, 0.5, 0.3, 0.1,
       0.3, 0.8, 1.0, 0.8, 0.3,
       0.5, 1.0, 1.0, 1.0, 0.5,
       0.3, 0.8, 1.0, 0.8, 0.3,
       0.1, 0.3, 0.5, 0.3, 0.1)
    arrayTile.map(x => x.toByte)
  }

  def get00(layer: RDD[(SpaceTimeKey, Tile)]): Unit ={
//    layer.map(x=>x._2).reduce((x,y)=>x.)
  }

  def get01(): Unit ={

  }

  def get10(): Unit ={

  }

  def get11(): Unit ={

  }
}
