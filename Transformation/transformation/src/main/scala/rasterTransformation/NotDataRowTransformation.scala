package rasterTransformation

import java.time.LocalDateTime

/**
  * Created by marc on 29.05.17.
  */
class NotDataRowTransformation(lon : Int,
                               lat : Int,
                               time : LocalDateTime,
                               var data : Boolean) extends RowTransformationTime(lon,lat,time){


}
