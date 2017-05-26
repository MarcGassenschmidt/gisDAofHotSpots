package scenarios

import java.io.{File, FileOutputStream, PrintWriter}

import au.com.bytecode.opencsv.CSVWriter
import clustering.ClusterHotSpots
import export.{SerializeTile, SoHResult, TileVisualizer}
import geotrellis.raster.Tile
import getisOrd.{GenericGetisOrd, GetisOrd, GetisOrdFocal, SoH}
import parmeters.Settings
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer

/**
  * Created by marc on 19.05.17.
  */
class DifferentRasterSizes extends GenericScenario{



  override def runScenario(): Unit ={
    val time = System.currentTimeMillis()
    val globalSettings =new Settings()
    globalSettings.fromFile = true
    globalSettings.scenario = "RasterSizes"
    val outPutResults = ListBuffer[SoHResult]()
    val runs = 10
    //bigger area
    //(40.567483, -74.091993) to (40.988291, -73.540960)
    globalSettings.shiftToPostive = 74.091993*globalSettings.multiToInt
    var latMin = 40.567483*globalSettings.multiToInt
    var lonMin = -74.091993*globalSettings.multiToInt+globalSettings.shiftToPostive
    var latMax = 40.988291*globalSettings.multiToInt
    var lonMax = -73.540960*globalSettings.multiToInt+globalSettings.shiftToPostive
    forFocalG(globalSettings, outPutResults, runs)
    forGlobalG(globalSettings, outPutResults, runs)
    saveResult(globalSettings, outPutResults)
    println("Total="+(System.currentTimeMillis()-time)/1000)
  }



  override def getParentChildSetting(global : Settings): (Settings, Settings) = {
    val para = new Settings()
    para.scenario = global.scenario
    para.weightRadius = 3
    para.focalRange = global.focalRange
    para.sizeOfRasterLat = global.sizeOfRasterLat
    para.sizeOfRasterLon = global.sizeOfRasterLon
    para.focal = global.focal
    val paraChild = new Settings()
    paraChild.scenario = global.scenario
    paraChild.focal = global.focal
    paraChild.focalRange = global.focalRange
    paraChild.sizeOfRasterLat = global.sizeOfRasterLat
    paraChild.sizeOfRasterLon = global.sizeOfRasterLon
    paraChild.parent = false
    paraChild.weightRadius = 2
    (para, paraChild)
  }








}
