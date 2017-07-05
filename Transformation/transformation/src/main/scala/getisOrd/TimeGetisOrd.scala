package getisOrd

import geotrellis.Spheroid
import geotrellis.raster.{DoubleRawArrayTile, MultibandTile, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.Extent
import importExport.{ImportGeoTiff, PathFormatter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import parmeters.Settings

import scala.collection.immutable.LinearSeq
import scala.collection.mutable

/**
  * Created by marc on 03.07.17.
  */
object TimeGetisOrd {

  def getMultibandFocalGetisOrd(multibandTile: MultibandTile, setting: Settings, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    null
  }

  def isInTile(x: Int, y: Int, mbT: MultibandTile): Boolean = {
    x>=0 && y>=0 && x<mbT.cols && y < mbT.rows
  }

  def getSum(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : Spheroid, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): Double = {

    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sum : Double = 0.0
    for (x <- -radius to radius) {
      for (y <- -radius to radius) {
        for(z <- -radiusTime to radiusTime){
        if (spheroid.isInRange(x,y,z)) {
          var cx = c + x
          var ry = r + y
          var bz = (b + z) % 24
          if (bz < 0) {
            bz += 24
          }
          if (isInTile(c + x, r + y, mbT)) {
            sum += mbT.band(bz).getDouble(cx, ry)
          } else {


            val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
            val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
            if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
              cx = cx % (mbT.cols - 1)
              if (cx < 0) {
                cx += mbT.cols
              }
              ry = ry % (mbT.rows - 1)
              if (ry < 0) {
                ry += mbT.rows
              }
              sum += neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)
            }
          }
        }
        }
      }
    }
    sum
  }

  def getSum(mbT: MultibandTile, spheroid : Spheroid, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val multibandTile: MultibandTile = getEmptyMultibandArray(mbT)

    for(b <- 0 to mbT.bandCount-1){
      val singleBand = multibandTile.band(b).asInstanceOf[DoubleRawArrayTile]
      for(c <- 0 to multibandTile.cols-1){
        for(r <- 0 to multibandTile.rows-1){
          singleBand.setDouble(c,r, getSum(b,c,r,mbT, spheroid, position, neigbours))
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

  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings, stats : StatsGlobal,position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val spheroid = Spheroid(setting.weightRadius, setting.weightRadiusTime)
    val RoW = getSum(multibandTile, spheroid, position,neigbours)
    println("End RoW")

    assert(multibandTile.cols==setting.layoutTileSize)
    assert(multibandTile.rows==setting.layoutTileSize)
    val extent = new Extent(0,0,setting.layoutTileSize, setting.layoutTileSize)

    val MW = stats.gM*spheroid.getSum() //W entry is allways 1
    val NW2 = stats.gN*spheroid.getSum() //W entry is allways 1
    val W2 = Math.pow(spheroid.getSum(),2)
    val denominator = stats.gS*Math.sqrt((NW2-W2)/(stats.gN-1))
    //TODO
    //assert(denominator>0)
    if(denominator>0){
      println(denominator+","+position.toString+","+stats.toString)

    }
    println("End Denminator")
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

    var fN : MultibandTile = null
    var fM : MultibandTile = null
    var fS : MultibandTile = null
    var st : StatsGlobal= null
    if(setting.focal){
      fN = null
      fM = null
      fS = null
    } else {
      //TODO if needed parrelisse
      st = getSTGlobal(origin)
    }
    var counter = 0
    println("calcualted stats")
    val broadcast = SparkContext.getOrCreate(setting.conf).broadcast(rdd.collect())
    println("broadcast ended")
    rdd.foreachPartition(iter=>{
      iter.foreach(x=>{
        var result : MultibandTile = null
        counter += 1
        println("Run for key:"+x._1.toString+" counter:"+counter)
        if(setting.focal){
          result = getMultibandFocalGetisOrd(x._2, setting,x._1,getNeigbours(x._1,broadcast))

        } else {
          println("starated G*")
          result = getMultibandGetisOrd(x._2, setting, st,x._1,getNeigbours(x._1,broadcast))
        }
        val extentForPartition = new Extent(
          setting.buttom._1+x._1._1*setting.layoutTileSize*setting.sizeOfRasterLon,
          setting.buttom._2+x._1._2*setting.layoutTileSize*setting.sizeOfRasterLat,
          setting.buttom._2+(x._1._1+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1,
          setting.buttom._2+(x._1._2+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1)
        val path = (new PathFormatter).getDirectory(setting, "partitions")
        println(path+x._1.toString+".tif")
        if(result==null){
          println("Result null, key:"+x._1.toString)
        } else if(setting.test) {
          println("Test no need to write, if needed change to no test?")
        } else {
            //(new ImportGeoTiff().writeMulitGeoTiff(result,extentForPartition,path+x._1.toString+".tif"))
            (new ImportGeoTiff().writeMulitTimeGeoTiffToSingle(result,extentForPartition,path+x._1.toString))
        }

        result
      })
    })
  }

  def getSTGlobal(origin : MultibandTile): StatsGlobal = {
    val gN = (origin.bandCount * origin.rows * origin.cols)

    val gM = origin.bands.map(x => x.toArrayDouble().filter(x => filterNoData(x)).reduce(_ + _)).reduce(_ + _)/gN

    val singelSDBand = origin.bands.map(x => x.toArrayDouble().filter(x => filterNoData(x)).map(x => Math.pow(x - gM, 2)).reduce(_ + _))
    val gS = Math.sqrt(singelSDBand.reduce(_ + _) * (1.0 / (gN.toDouble - 1.0)))
    new StatsGlobal(gN,gM,gS)
  }

  def isInRange(newKey: SpatialKey, max : SpatialKey): Boolean = {
    val r = newKey._2>=0 && newKey._1>=0 && newKey._1<=max._1 && newKey._2<=max._2
    r
  }

  def getNeigbours(key : SpatialKey, broadcast: Broadcast[Array[(SpatialKey, MultibandTile)]]): mutable.HashMap[SpatialKey,MultibandTile] ={
    println("starated Neighbours*")
    val max = broadcast.value.map(x=>x._1).max
    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    for(i <- -1 to 1) {
      for (j <- -1 to 1) {
        val newKey = new SpatialKey(key._1 + i, key._2 + j)
        if(isInRange(newKey, max) && Math.abs(i)+Math.abs(j)!=0){
          val lookUp = broadcast.value.filter(y=>y._1==newKey)
          lookUp.map(x=>hashMap.put(x._1,x._2))
        }

      }
    }
    println("End Neighbours*")
    return hashMap
  }

}

class StatsFocal(val gN : MultibandTile, val gM : MultibandTile, val gS :MultibandTile) extends Serializable{

}

class StatsGlobal(val gN : Int, val gM : Double, val gS :Double) extends Serializable{
  override def toString: String = "stats(N,M,S):("+gN+","+gM+","+gS+")"
}
