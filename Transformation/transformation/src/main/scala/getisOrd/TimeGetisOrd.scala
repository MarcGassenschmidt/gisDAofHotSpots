package getisOrd

import geotrellis.Spheroid
import geotrellis.raster.{DoubleRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import importExport.ImportGeoTiff
import org.apache.spark.rdd.RDD
import parmeters.Settings

/**
  * Created by marc on 03.07.17.
  */
object TimeGetisOrd {

  def getMultibandFocalGetisOrd(multibandTile: MultibandTile, setting: Settings): MultibandTile = {
    null
  }

  def isInTile(x: Int, y: Int, mbT: MultibandTile): Boolean = {
    x>0 && y>0 && x<mbT.cols && y < mbT.rows
  }

  def getSum(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : Spheroid): Double = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sum : Double = 0.0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime)
        if (spheroid.isInRange(x,y,z)) {
          val cx = c+x
          val ry = r+y
          if(isInTile(c+x,r+y, mbT)){
            var bz = (b+z) % 24
            if(bz<0){
              bz+=24
            }
            sum += mbT.band(bz).getDouble(cx,ry)
          } else {
            //TODO getData from NeigbourTile
          }

        }
      }
    }
    sum
  }

  def getSum(mbT: MultibandTile, spheroid : Spheroid): MultibandTile = {
    val multibandTile: MultibandTile = getEmptyMultibandArray(mbT)

    for(b <- 0 to mbT.bandCount-1){
      val singleBand = multibandTile.band(b).asInstanceOf[DoubleRawArrayTile]
      for(c <- 0 to multibandTile.cols-1){
        for(r <- 0 to multibandTile.rows-1){
          singleBand.setDouble(c,r, getSum(b,c,r,mbT, spheroid))
        }
      }
    }
    multibandTile

  }

  def getEmptyMultibandArray(mbT: MultibandTile): MultibandTile = {
    val bands = mbT.bandCount
    val size = mbT.size
    var bandArray = new Array[Tile](bands)
    for (b <- 0 to bands-1) {
      bandArray(b) = new DoubleRawArrayTile(Array.fill(size)(0), mbT.rows, mbT.cols)
    }
    val multibandTile = MultibandTile.apply(bandArray)
    multibandTile
  }

//  def getMean(multibandTile: MultibandTile, setting: Settings) = MultibandTile {
//    //getSum(multibandTile, new Spheroid())
//  }

  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings): MultibandTile = {
    val spheroid = Spheroid(setting.weightRadius, setting.weightRadiusTime)
    val RoW = getSum(multibandTile, spheroid)
    val N = (multibandTile.bandCount*multibandTile.rows*multibandTile.cols)
    val M = multibandTile.bands.map(x=>x.toArrayDouble().reduce(_+_)).reduce(_+_)/N
    val S = multibandTile.bands.map(x=>x.toArrayDouble().map(x=>Math.pow(x-M,2)).reduce(_+_)).reduce(_+_)
    val MW = M*spheroid.getSum() //W entry is allways 1
    val NW2 = N*spheroid.getSum() //W entry is allways 1
    val W2 = Math.pow(spheroid.getSum(),2)
    val denominator = S*Math.sqrt((NW2-W2)/(N-1))
    assert(denominator>0)
    RoW.mapBands((band:Int,tile:Tile)=>tile.mapDouble(x=>(x-MW)/denominator))
    RoW
  }

  def getGetisOrd(rdd : RDD[(SpatialKey, MultibandTile)], setting : Settings): Unit ={
    val keys = rdd.keys.collect().max //TODO
    val keyCount = rdd.keys.count()
    val band = rdd.first()._2.band(0)
    val x = band.getDouble(0,0)
    rdd.foreach(f=>f)
    rdd.foreachPartition(f=>f)
    rdd.foreachPartition(iter=>{
      iter.map(x=>{
        var result : MultibandTile = null
        if(setting.focal){
          result = getMultibandFocalGetisOrd(x._2, setting)
        } else {
          result = getMultibandGetisOrd(x._2, setting)
        }
        (new ImportGeoTiff().writeMulitGeoTiff(result,setting,0,0,x._1.toString))
        result
      })
    })
  }

}
