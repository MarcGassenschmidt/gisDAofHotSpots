package osm

import parmeters.Parameters

/**
  * Created by marc on 16.05.17.
  */
class ConvertPositionToCoordinate {


  def getGPSCoordinate(rowLat : Int, colLon : Int, para : Parameters): (Double, Double) = {
    val latScaled =(rowLat*(para.sizeOfRasterLat))
    val lonScaled = (colLon*(para.sizeOfRasterLon))
    val lat = (latScaled+para.latMin)/para.multiToInt
    val lon = (lonScaled+(para.lonMin-para.shiftToPostive))/para.multiToInt
    (lat,lon)
  }
}
