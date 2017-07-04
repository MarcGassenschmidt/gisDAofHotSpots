package getisOrd

import geotrellis.Spheroid
import geotrellis.raster.{DoubleRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.Extent
import importExport.{ImportGeoTiff, PathFormatter}
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

  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings, N : Int, M : Double, S: Double): MultibandTile = {
    val spheroid = Spheroid(setting.weightRadius, setting.weightRadiusTime)
    val RoW = getSum(multibandTile, spheroid)

    assert(multibandTile.cols==setting.layoutTileSize)
    assert(multibandTile.rows==setting.layoutTileSize)
    val extent = new Extent(0,0,setting.layoutTileSize, setting.layoutTileSize)

    val MW = M*spheroid.getSum() //W entry is allways 1
    val NW2 = N*spheroid.getSum() //W entry is allways 1
    val W2 = Math.pow(spheroid.getSum(),2)
    val denominator = S*Math.sqrt((NW2-W2)/(N-1))
    assert(denominator>0)
    RoW.mapBands((band:Int,tile:Tile)=>tile.mapDouble(x=>(x-MW)/denominator))
    RoW
  }

  def filterNoData(f: Double): Boolean = {
    val r = (f== -2147483648 ||f.isNaN || f.isNegInfinity || f.isInfinity)
    !r
  }

  def getGetisOrd(rdd : RDD[(SpatialKey, MultibandTile)], setting : Settings, origin : MultibandTile): Unit ={
    val keys = rdd.keys.collect().max //TODO
    val keyCount = rdd.keys.count()
    val band = rdd.first()._2.band(0)
    var gN = 0
    var gM = 0.0
    var gS = 0.0
    var fN : MultibandTile = null
    var fM : MultibandTile = null
    var fS : MultibandTile = null
    if(setting.focal){
      fN = null
      fM = null
      fS = null
    } else {
      //TODO if needed parrelisse
      gN = (origin.bandCount*origin.rows*origin.cols)

      gM = origin.bands.map(x=>x.toArrayDouble().filter(x=>filterNoData(x)).reduce(_+_)).reduce(_+_)

      val singelSDBand = origin.bands.map(x=>x.toArrayDouble().filter(x=>filterNoData(x)).map(x=>Math.pow(x-gM,2)).reduce(_+_))
      gS = Math.sqrt(singelSDBand.reduce(_+_)*(1.0/(gN.toDouble-1.0)))
    }


    rdd.foreachPartition(iter=>{
      iter.foreach(x=>{
        var result : MultibandTile = null

        if(setting.focal){
          result = getMultibandFocalGetisOrd(x._2, setting)
        } else {
          result = getMultibandGetisOrd(x._2, setting, gN, gM, gS)
        }
        val extentForPartition = new Extent(
          setting.buttom._1+x._1._1*setting.layoutTileSize*setting.sizeOfRasterLon,
          setting.buttom._2+x._1._2*setting.layoutTileSize*setting.sizeOfRasterLat,
          setting.buttom._2+(x._1._1+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1,
          setting.buttom._2+(x._1._2+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1)
        val path = (new PathFormatter).getDirectory(setting, "partitions")
        println(path+x._1.toString+".tif")
        (new ImportGeoTiff().writeMulitGeoTiff(result,extentForPartition,path+x._1.toString+".tif"))
        result
      })
    })
  }

}
