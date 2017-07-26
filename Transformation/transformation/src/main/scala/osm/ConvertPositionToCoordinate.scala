package osm

import parmeters.Settings

/**
  * Created by marc on 16.05.17.
  */
object ConvertPositionToCoordinate {


  def getGPSCoordinate(rowLat : Int, colLon : Int, para : Settings): (Double, Double) = {
    val latScaled =(rowLat*(para.sizeOfRasterLat))
    val lonScaled = (colLon*(para.sizeOfRasterLon))
    val lat = (latScaled+para.latMin)/para.multiToInt
    val lon = (lonScaled+(para.lonMin-para.shiftToPostive))/para.multiToInt
    (lat,lon)
  }
}
