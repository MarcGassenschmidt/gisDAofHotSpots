package rasterTransformation

import org.joda.time.DateTime

/**
  * Created by marc on 29.05.17.
  */
class NotDataRowTransformation(lon : Int,
                               lat : Int,
                               time : DateTime,
                               var data : Boolean) extends RowTransformationTime(lon,lat,time){


}
