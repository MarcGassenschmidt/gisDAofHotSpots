package export

import geotrellis.raster.Tile
import getisOrd.Weight.Weight
import parmeters.Settings

/**
  * Created by marc on 11.05.17.
  */
class SoHResult(parent : Tile, weight : Tile, wParent : Settings, time : Long, sohValue : (Double,Double), lat : Int) {

  def copySettings(): Settings = {
    val set = new Settings
    set.sizeOfRasterLat = wParent.sizeOfRasterLat
    set.focal = wParent.focal
    set.focalRange = wParent.focalRange
    set.weightMatrix = wParent.weightMatrix
    set.weightRadius = wParent.weightRadius
    set
  }

  val localSet = copySettings()

  def format(shortFormat : Boolean): String = {
    if(shortFormat){
      return formatShort()
    }
    val parentString  = lat+","+localSet.focal+","+localSet.focalRange+","+parent.cols+","+parent.rows+","+localSet.weightMatrix+","+localSet.weightRadius
    //val childString = wChild.sizeOfRasterLat+","+wChild.focal+","+wChild.weightMatrix+","+wChild.weightRadius
    return parentString+","+time+","+sohValue._1+","+sohValue._2+","

  }

  def formatShort(): String = {
    return getLat+","+sohValue._1+","+sohValue._2
  }

  def headerShort() : String = {
    "rasterSize(meters),downward-"+localSet.focal+"-"+localSet.focalRange+",upward-"+localSet.focal+"-"+localSet.focalRange+",downwardInverse-"+localSet.focal+"-"+localSet.focalRange+",upwardInverse-"+localSet.focal+"-"+localSet.focalRange+","
  }

  def getLat() : Int = {
    lat
  }

  def header(shortFormat : Boolean): String ={
    if(shortFormat){
      return headerShort()
    }
    "rasterSize(meters),parentFocal,focalRange,cols,rows,weighParent,weightParentRadius,duration(seconds),downward"+wParent.focal+"-"+wParent.focalRange+",upward"+wParent.focal+"-"+wParent.focalRange+","
  }

  def getSohUp(): Double = {
    sohValue._1
  }

  def getSohDown(): Double = {
    sohValue._2
  }






}
