package getisOrd

import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Circle

/**
  * Created by marc on 22.05.17.
  */
class GenericGetisOrd {

  def genericGStar(lR : Tile, lW : Tile, lN: Int, lM :Tile, lS : Tile) : Tile = {
    (lR.focalSum(Circle(lW.cols/2))-lM*lW)/(lS*Math.sqrt((lN*getPowerOfTwoForElementsAsSum(lW)-getSummForTile(lW)*getSummForTile(lW))/(lN-1)))
  }

  def getPowerOfTwoForElementsAsSum(tile : Tile): Double ={
    tile.toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
  }

  def getSummForTile(tile: Tile): Double ={
    tile.toArrayDouble().reduce(_+_)
  }
}
