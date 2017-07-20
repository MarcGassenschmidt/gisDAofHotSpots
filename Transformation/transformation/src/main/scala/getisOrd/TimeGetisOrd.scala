package getisOrd

import geotrellis.Spheroid
import geotrellis.raster.stitch.Stitcher.MultibandTileStitcher
import geotrellis.raster.{DoubleRawArrayTile, GridBounds, MultibandTile, Tile, TileLayout}
import geotrellis.spark.SpatialKey
import geotrellis.vector.Extent
import importExport.{ImportGeoTiff, PathFormatter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable

/**
  * Created by marc on 03.07.17.
  */
object TimeGetisOrd {
  var timeMeasuresFocal = new MeasuersFocal()
  var timeMeasuresGlobal = new MeasuersGloabl()

  def getMultibandFocalGetisOrd(mbT: MultibandTile, setting: Settings, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val spheroidFocal = new Spheroid(setting.focalRange, setting.focalRangeTime)
    val spheroidWeight = new Spheroid(setting.weightRadius, setting.weightRadiusTime)
    val radius = spheroidFocal.a
    val radiusTime = spheroidFocal.c
    val FocalGStar: MultibandTile = MultibandUtils.getEmptyMultibandArray(mbT)

    for (r <- 0 to mbT.rows - 1) {
      println("Next r:"+r)
      var start = System.currentTimeMillis()
      for (c <- 0 to mbT.cols - 1) {
        for (b <- 0 to mbT.bandCount - 1) {
          var startBand = System.currentTimeMillis()
          val RMWNW2 = getRMWNW2(b, c, r, mbT, spheroidWeight, position, neigbours, getNM(b, c, r, mbT, spheroidFocal, position, neigbours))
          val sd = getSD(b, c, r, mbT, spheroidFocal, position, neigbours, RMWNW2.M)
          FocalGStar.band(b).asInstanceOf[DoubleRawArrayTile].setDouble(c,r,RMWNW2.getGStar(sd))
          timeMeasuresFocal.addBand(System.currentTimeMillis()-startBand)
        }

      }
      if(position._1==0 && position._2==0)
        timeMeasuresFocal.addRow(System.currentTimeMillis()-start)

    }
    FocalGStar
  }




  def getSD(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : Spheroid, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile], mean : Double): Double = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sd : Double = 0.0
    var n = 0
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
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              sd += Math.pow(mbT.band(bz).getDouble(cx, ry)-mean,2)
              n += 1
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
                sd += Math.pow(neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)-mean,2)
                n += 1
              }
            }
          }
        }
      }
    }
    Math.sqrt(sd*(1.0 / (n.toDouble - 1.0)))
  }

  def getRMWNW2(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : Spheroid, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile], nm : (Int,Double)): StatsRNMW = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var row : Double = 0.0
    var wSum : Int= 0
    var start = System.currentTimeMillis()
    var neighbour = false
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
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              row += mbT.band(bz).getDouble(cx, ry)
              wSum += 1
            } else {
              val xShift = if (mbT.cols - 1 < cx) 1 else if (cx < 0) -1 else 0
              val yShift = if (mbT.rows - 1 < ry) 1 else if (ry < 0) -1 else 0
              if (neigbours.contains((position._1 + xShift, position._2 + yShift))) {
                neighbour = true
                cx = cx % (mbT.cols - 1)
                if (cx < 0) {
                  cx += mbT.cols
                }
                ry = ry % (mbT.rows - 1)
                if (ry < 0) {
                  ry += mbT.rows
                }
                row += neigbours.get((position._1 + xShift, position._2 + yShift)).get.band(bz).getDouble(cx, ry)
                wSum += 1
              }
            }
          }
        }
      }
    }

    val mw = wSum*nm._2
    val nw2 = wSum*nm._1
    val w2 =Math.pow(wSum,2)

    if(neighbour){
     timeMeasuresFocal.addNeighbour(System.currentTimeMillis()-start)
    } else {
     timeMeasuresFocal.addNoNeighbour(System.currentTimeMillis()-start)
    }

    new StatsRNMW(row,nm._1,nm._2,mw,nw2,w2)
  }

  def getNM(b: Int, c: Int, r: Int, mbT: MultibandTile, spheroid : Spheroid, position : SpatialKey, neigbours: mutable.HashMap[SpatialKey, MultibandTile]): (Int,Double) = {
    val radius = spheroid.a
    val radiusTime = spheroid.c
    var sum : Double = 0.0
    var count : Int= 0
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
            if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
              sum += mbT.band(bz).getDouble(cx, ry)
              count += 1
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
                count += 1
              }
            }
          }
        }
      }
    }
    (count,sum/count.toDouble)
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
          if (MultibandUtils.isInTile(c + x, r + y, mbT)) {
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
    val multibandTile: MultibandTile = MultibandUtils.getEmptyMultibandArray(mbT)

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



  def getMultibandGetisOrd(multibandTile: MultibandTile, setting: Settings, stats : StatsGlobal, position : SpatialKey, neighbours: mutable.HashMap[SpatialKey, MultibandTile]): MultibandTile = {
    val spheroid = Spheroid(setting.weightRadius, setting.weightRadiusTime)

    var start = System.currentTimeMillis()
    val RoW = getSum(multibandTile, spheroid, position,neighbours)
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setRoW(System.currentTimeMillis()-start)


    println("End RoW")

    assert(multibandTile.cols==setting.layoutTileSize._1)
    assert(multibandTile.rows==setting.layoutTileSize._2)
    val extent = new Extent(0,0,setting.layoutTileSize._1, setting.layoutTileSize._2)

    start = System.currentTimeMillis()
    val MW = stats.gM*spheroid.getSum() //W entry is allways 1
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setMW(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val NW2 = stats.gN.toLong*spheroid.getSum().toLong //W entry is allways 1
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setNW2(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val W2 = Math.pow(spheroid.getSum(),2)
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setW2(System.currentTimeMillis()-start)

    start = System.currentTimeMillis()
    val denominator = stats.gS*Math.sqrt((NW2-W2)/(stats.gN-1))
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setDenominator(System.currentTimeMillis()-start)

    //TODO
    assert(denominator>0)
    if(denominator>0){
      println(denominator+","+position.toString+","+stats.toString)

    }
    println("End Denminator")

    start = System.currentTimeMillis()
    val res = RoW.mapBands((band:Int,tile:Tile)=>tile.mapDouble(x=>{
      var result =((x-MW)/denominator)
      if(!isNotNaN(result)){
        result = 0
      }
      result
    }))
    if(position._1==0 && position._2==0)
      timeMeasuresGlobal.setDevision(System.currentTimeMillis()-start)

    res
  }

  def isNotNaN(f: Double): Boolean = {
    val r = (f== -2147483648 ||f.isNaN || f.isNegInfinity || f.isInfinity)
    !r
  }



  def getGetisOrd(rdd : RDD[(SpatialKey, MultibandTile)], setting : Settings, origin : MultibandTile): MultibandTile ={
    timeMeasuresFocal = new MeasuersFocal
    timeMeasuresGlobal = new MeasuersGloabl
    val start = System.currentTimeMillis()
    val keys = rdd.keys.collect().max
    val keyCount = rdd.keys.count()
    val band = rdd.first()._2.band(0)

    var st : StatsGlobal= null
    if(!setting.focal){
      val startStats = System.currentTimeMillis()
      st = getSTGlobal(origin)
      timeMeasuresGlobal.setStats(System.currentTimeMillis()-startStats)
    }
    var counter = 0
    println("calcualted stats")
    val startBroadcast = System.currentTimeMillis()
    val broadcast = SparkContext.getOrCreate(setting.conf).broadcast(rdd.collect())
    timeMeasuresFocal.setBroadCast(System.currentTimeMillis()-startBroadcast)
    timeMeasuresGlobal.setBroadCast(System.currentTimeMillis()-startBroadcast)
    println("broadcast ended")

    val tiles = rdd.map(x=>{
        var result : MultibandTile = null
        counter += 1
        println("Run for key:"+x._1.toString+" counter:"+counter)
        if(setting.focal){
          result = getMultibandFocalGetisOrd(x._2, setting,x._1,getNeigbours(x._1,broadcast))

        } else {
          println("started G*")
          result = getMultibandGetisOrd(x._2, setting, st,x._1,getNeigbours(x._1,broadcast))
        }
//        val extentForPartition = new Extent(
//          setting.buttom._1+x._1._1*setting.layoutTileSize*setting.sizeOfRasterLon,
//          setting.buttom._2+x._1._2*setting.layoutTileSize*setting.sizeOfRasterLat,
//          setting.buttom._2+(x._1._1+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1,
//          setting.buttom._2+(x._1._2+1)*setting.layoutTileSize*setting.sizeOfRasterLat-1)
//        val path = (new PathFormatter).getDirectory(setting, "partitions")
//        println(path+x._1.toString+".tif")
//        if(result==null){
//          println("Result null, key:"+x._1.toString)
//        } else if(setting.test) {
//          println("Test no need to write, if needed change to no test?")
//        } else {
//          //(new ImportGeoTiff().writeMulitGeoTiff(result,extentForPartition,path+x._1.toString+".tif"))
//          //  (new ImportGeoTiff().writeMulitTimeGeoTiffToSingle(result,extentForPartition,path+x._1.toString))
//        }
        println("Finsished:"+x._1.toString+" counter:"+counter)
        (x._1,result)
    })
    val n = tiles.count()
    print("Counter:"+n)

    tiles.collect().map(x=>println("c,r"+x._2.cols+","+x._2.rows))

    var raster = tiles.stitch()
//    val test = MultibandUtils.getEmptyMultibandArray(origin)
//    var start1 = System.currentTimeMillis()
//    test.mapBands((f : Int, t : Tile) =>t.mapDouble((x:Int,y:Int,v:Double)=>raster.band(f).getDouble(x,y)))
//    println("Time for own split"+System.currentTimeMillis()-start1)
//    val split = raster.split(new TileLayout(origin.cols,origin.rows,origin.cols,origin.rows))(0)
    println("Raster,c,r"+raster.cols+","+raster.rows)
    val path = (new PathFormatter).getDirectory(setting, "partitions")
    assert(raster.dimensions==origin.dimensions)
    //(new ImportGeoTiff().writeMulitGeoTiff(tiles,setting,path+"all.tif"))
    val startWriting = System.currentTimeMillis()

    (new ImportGeoTiff().writeMultiTimeGeoTiffToSingle(raster,setting,path+"all.tif"))
    println("-----------------------------------------------------------------Start------------" +
      "---------------------------------------------------------------------------------------------------------")
    if(setting.focal){
      timeMeasuresFocal.setWriting(System.currentTimeMillis()-startWriting)
      timeMeasuresFocal.setAll(System.currentTimeMillis()-start)
      println(timeMeasuresFocal.getPerformanceMetrik())
    } else {
      timeMeasuresGlobal.setWriting(System.currentTimeMillis()-startWriting)
      timeMeasuresGlobal.setAllTime(System.currentTimeMillis()-start)
      println(timeMeasuresGlobal.getPerformanceMetrik())
    }
    println("------------------------------------------------------------------End----------" +
      "----------------------------------------------------------------------------------------------------------")

    raster
  }

  def getSTGlobal(origin : MultibandTile): StatsGlobal = {
    val gN = (origin.bandCount * origin.rows * origin.cols)

    val gM = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).reduce(_ + _)).reduce(_ + _)/gN

    val singelSDBand = origin.bands.map(x => x.toArrayDouble().filter(x => isNotNaN(x)).map(x => Math.pow(x - gM, 2)).reduce(_ + _))
    val gS = Math.sqrt((singelSDBand.reduce(_ + _)) * (1.0 / (gN.toDouble - 1.0)))
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

  class MeasuersGloabl(){
    var statsTime = 0.0
    var allTime = 0.0
    var RoW = 0.0
    var MW = 0.0
    var NW2 = 0.0
    var W2 = 0.0
    var denominator = 0.0
    var devision = 0.0
    var writing = 0.0
    var broadCast = 0.0
    var collect = 0.0
    var stitch = 0.0

    def setcollect(time : Long): Unit ={
      collect = time
    }

    def setStitch(time : Long): Unit ={
      stitch = time
    }



    def setWriting(time : Long): Unit ={
      writing = time
    }

    def setStats(time : Long): Unit ={
      statsTime = time
    }

    def setAllTime(time : Long): Unit ={
      allTime = time
    }

    def setRoW(time : Long): Unit ={
      RoW = time
    }

    def setMW(time : Long): Unit ={
      MW = time
    }

    def setNW2(time : Long): Unit ={
      NW2 = time
    }

    def setW2(time : Long): Unit ={
      W2 = time
    }

    def setBroadCast(time : Long): Unit ={
      broadCast = time
    }

    def setDenominator(time : Long): Unit ={
      denominator = time
    }

    def setDevision(time : Long): Unit ={
      devision = time
    }

    def getPerformanceMetrik(): String ={
      val out = "Time for G* was:"+allTime+
        "\n time for stats:"+statsTime+
        "\n time for RoW:"+RoW+
        "\n time for MW:"+MW+
        "\n time for NW2:"+NW2+
        "\n time for W2:"+W2+
        "\n time for Denominator:"+denominator+
        "\n time for Devision:"+devision+
        "\n time for Writing:"+writing+
      "\n time for Broadcast:"+broadCast+
        "\n time for collect:"+collect+
        "\n time for stitch:"+stitch
      out
    }
  }



  class MeasuersFocal(){
    var neighbour = 0.0
    var neighbourCount = 0
    var all = 0.0
    var noNeighbour = 0.0
    var noNeighbourCount = 0
    var row = 0.0
    var rowCount = 0
    var band = 0.0
    var bandCount = 0
    var writing = 0.0
    var broadCast = 0.0
    var collect = 0.0
    var stitch = 0.0

    def setcollect(time : Long): Unit ={
      collect = time
    }

    def setStitch(time : Long): Unit ={
      stitch = time
    }
    def addRow(time : Double): Unit ={
      row += time
      rowCount += 1
    }

    def addNeighbour(time : Double): Unit ={
      neighbour += time
      neighbourCount += 1
    }

    def addNoNeighbour(time : Double): Unit ={
      noNeighbour += time
      noNeighbourCount += 1
    }

    def addBand(time : Double): Unit ={
      band += time
      bandCount += 1
    }

    def setAll(all : Double): Unit = {
      this.all = all
    }

    def setWriting(time : Double): Unit = {
      this.writing = time
    }

    def setBroadCast(time : Long): Unit ={
      broadCast = time
    }

    def getPerformanceMetrik(): String ={
      val out = "Time for focal G* was:"+all+
        "\n time for neigbour:"+neighbour/neighbourCount.toDouble+
        "\n time for NoNeigbour:"+noNeighbour/noNeighbourCount.toDouble+
        "\n time for Band:"+band/bandCount.toDouble+
        "\n time for Row:"+row/rowCount.toDouble+
          "\n time for Writing:"+writing+
          "\n time for Broadcast:"+broadCast+
          "\n time for collect:"+collect+
          "\n time for stitch:"+stitch
      out
    }
  }

}

class StatsRNMW(val RoW : Double, val N : Double, val M : Double, val MW : Double, val NW2 : Double, val W2 : Double) extends Serializable{
  def getNominator(): Double ={
    RoW-MW
  }

  def getDenominator(sd : Double): Double ={
    sd*Math.sqrt((NW2-W2)/(N-1))
  }

  def getGStar(sd : Double) : Double = {
    if(sd==0){
      return 0
    }
    val r = getNominator()/getDenominator(sd)
    if(TimeGetisOrd.isNotNaN(r)){
      return r
    } else {
      return 1
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[StatsRNMW]){
      val compa = obj.asInstanceOf[StatsRNMW]
      return compa.M==this.M &&
              compa.N==this.N &&
              compa.RoW==this.RoW &&
              compa.MW==this.MW &&
              compa.NW2==this.NW2 &&
              compa.W2 == this.W2
    } else {
      return false
    }
  }
}

class StatsFocal(val fN : MultibandTile, val fM : MultibandTile, val fS :MultibandTile, val MW: MultibandTile, val NW2: MultibandTile, val W2 : MultibandTile) extends Serializable{

}

class StatsGlobal(val gN : Int, val gM : Double, val gS :Double) extends Serializable{
  override def toString: String = "stats(N,M,S):("+gN+","+gM+","+gS+")"
}
