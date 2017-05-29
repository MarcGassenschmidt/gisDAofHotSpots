package rasterTransformation


import org.joda.time.DateTime
/**
  * Created by marc on 10.05.17.
  */
class RowTransformationTime(lon : Int,
                            lat : Int,
                            var time : DateTime)  extends RowTransformation(lon, lat){

}
