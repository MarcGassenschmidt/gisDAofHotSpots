package getisOrd

import geotrellis.raster.{ArrayTile, IntRawArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.Circle
import parmeters.Settings

/**
  * Created by marc on 22.05.17.
  */
class GenericGetisOrd {

  def genericGStar(lR : Tile, lW : Tile, lN: Tile, lM :Tile, lS : Tile) : Tile = {
    (lR.focalSum(Circle(lW.cols/2))-lM*lW)/(lS*((lN*getPowerOfTwoForElementsAsSum(lW)-getSummForTile(lW)*getSummForTile(lW))/(lN-1)).mapDouble(x => Math.sqrt(x)))
  }

  def getPowerOfTwoForElementsAsSum(tile : Tile): Double ={
    tile.toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
  }

  def getSummForTile(tile: Tile): Double ={
    tile.toArrayDouble().reduce(_+_)
  }

  def createNewWeight(para : Settings) : Tile = {
    getWeightMatrixSquare(para.weightRadius/2)
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
}
