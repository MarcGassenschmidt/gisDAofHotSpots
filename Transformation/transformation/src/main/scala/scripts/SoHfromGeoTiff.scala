package scripts

import java.io.PrintWriter

import clustering.ClusterHotSpots
import export.TileVisualizer
import getisOrd.SoH
import importExport.ImportGeoTiff
import parmeters.Settings


/**
  * Created by marc on 07.06.17.
  */
object SoHfromGeoTiff {

  def main(args: Array[String]): Unit = {
    println("Started")
    getSoH()
  }

  def getSoH(): Unit ={
    val imp = new ImportGeoTiff()
    var format = "/home/marc/media/SS_17/focalgstar-newyork/data/blur/"
    val globalSettings = new Settings()
    var pw = new PrintWriter("/home/marc/media/SS_17/focalgstar-newyork/data/blur_soh.csv")
//    pw.println("-------------------focalGstar----blur---------------------")
    pw.println("Weight,DownFocalBlur,UpFocalBlur")

    for(i <- 0 to 10){
      val child = imp.getGeoTiff(format+"focalgstar-221x213-z03-w"+(7+i*2).formatted("%02d")+".tif", globalSettings)
      val parent = imp.getGeoTiff(format+"focalgstar-221x213-z03-w"+(7+(i+1)*2).formatted("%02d")+".tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val vis = new TileVisualizer()
      globalSettings.parent = false
      globalSettings.focal = true
      globalSettings.weightRadius = (7+(i+1)*2)
      vis.visualTileNew(clusterChild._1, globalSettings, "cluster")
      globalSettings.parent = true
      vis.visualTileNew(clusterParent._1, globalSettings, "cluster")

      val sohVal :(Double,Double) = SoH.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((7+(i+1)*2).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }


//    pw.println("-------------------gStar---blur----------------------")
    pw.println("Weight,DownBlur,UpBlur")
    for(i <- 0 to 10){
      val child = imp.getGeoTiff(format+"gstar-221x213-z03-w"+(7+i*2).formatted("%02d")+".tif", globalSettings)
      val parent = imp.getGeoTiff(format+"gstar-221x213-z03-w"+(7+(i+1)*2).formatted("%02d")+".tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val vis = new TileVisualizer()
      globalSettings.parent = false
      globalSettings.focal = false
      globalSettings.weightRadius = (7+(i+1)*2)
      vis.visualTileNew(clusterChild._1, globalSettings, "cluster")
      globalSettings.parent = true
      vis.visualTileNew(clusterParent._1, globalSettings, "cluster")

      val sohVal :(Double,Double) = SoH.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((7+(i+1)*2).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }

    pw.flush()
    pw.close()
    pw = new PrintWriter("/home/marc/media/SS_17/focalgstar-newyork/data/zoom_soh.csv")
    format = "/home/marc/media/SS_17/focalgstar-newyork/data/zoom/"

    //pw.println("-------------------focalGstar------zoom-------------------")
    pw.println("Zoom,DownFocalZoom,UpFocalZoom")
    for(i <- 1 to 7){

      val sizeF = (661.0/(i)).ceil.toInt.formatted("%03d")+"x"+(639.0/(i)).ceil.toInt.formatted("%03d")
      val sizeF1 = (661.0/(i+1)).ceil.toInt.formatted("%03d")+"x"+(639.0/(i+1)).ceil.toInt.formatted("%03d")
      val child = imp.getGeoTiff(format+"focalgstar-"+sizeF+"-z"+i.formatted("%02d")+"-w11.tif", globalSettings)
      val parent = imp.getGeoTiff(format+"focalgstar-"+sizeF1+"-z"+(i+1).formatted("%02d")+"-w11.tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)


      val sohVal :(Double,Double) = SoH.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((i+1).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }


    //pw.println("-------------------gStar-------zoom------------------")
    pw.println("Zoom,DownZoom,UpZoom")
    for(i <- 1 to 7){
      val sizeF = (661.0/(i)).ceil.toInt.formatted("%03d")+"x"+(639.0/(i)).ceil.toInt.formatted("%03d")
      val sizeF1 = (661.0/(i+1)).ceil.toInt.formatted("%03d")+"x"+(639.0/(i+1)).ceil.toInt.formatted("%03d")
      val child = imp.getGeoTiff(format+"gstar-"+sizeF+"-z"+i.formatted("%02d")+"-w11.tif", globalSettings)
      val parent = imp.getGeoTiff(format+"gstar-"+sizeF1+"-z"+(i+1).formatted("%02d")+"-w11.tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)


      val sohVal :(Double,Double) = SoH.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((i+1).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }
    pw.flush()
    pw.close()
  }
}


