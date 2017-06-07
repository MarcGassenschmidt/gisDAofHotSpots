package scripts

import java.io.PrintWriter

import clustering.ClusterHotSpots
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
    pw.println("-------------------focalGstar----blur---------------------")
    pw.println("Weight,Down,Up")

    for(i <- 0 to 7){
      val child = imp.getGeoTiff(format+"focalgstar-221x213-w"+(7+i*2).formatted("%02d")+".tif", globalSettings)
      val parent = imp.getGeoTiff(format+"focalgstar-221x213-w"+(7+(i+1)*2).formatted("%02d")+".tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)

      val soh = new SoH()
      val sohVal :(Double,Double) = soh.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((7+i*2).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }


    pw.println("-------------------gStar---blur----------------------")
    pw.println("Weight,Down,Up")
    for(i <- 0 to 7){
      val child = imp.getGeoTiff(format+"gstar-221x213-w"+(7+i*2).formatted("%02d")+".tif", globalSettings)
      val parent = imp.getGeoTiff(format+"gstar-221x213-w"+(7+(i+1)*2).formatted("%02d")+".tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)

      val soh = new SoH()
      val sohVal :(Double,Double) = soh.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((7+i*2).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }

    pw.flush()
    pw.close()
    pw = new PrintWriter("/home/marc/media/SS_17/focalgstar-newyork/data/zoom_soh.csv")
    format = "/home/marc/media/SS_17/focalgstar-newyork/data/zoom/"

    pw.println("-------------------focalGstar------zoom-------------------")
    pw.println("Zooom,Down,Up")
    for(i <- 1 to 7){
      val child = imp.getGeoTiff(format+"focalgstar-z"+i.formatted("%02d")+"-w11.tif", globalSettings)
      val parent = imp.getGeoTiff(format+"focalgstar-z"+(i+1).formatted("%02d")+"-w11.tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)

      val soh = new SoH()
      val sohVal :(Double,Double) = soh.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((i).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }


    pw.println("-------------------gStar-------zoom------------------")
    pw.println("Zoom,Down,Up")
    for(i <- 1 to 7){
      val child = imp.getGeoTiff(format+"gstar-z"+i.formatted("%02d")+"-w11.tif", globalSettings)
      val parent = imp.getGeoTiff(format+"gstar-z"+(i+1).formatted("%02d")+"-w11.tif", globalSettings)
      val clusterParent = (new ClusterHotSpots(child)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)
      val clusterChild = (new ClusterHotSpots(parent)).findClusters(globalSettings.clusterRange, globalSettings.critivalValue)

      val soh = new SoH()
      val sohVal :(Double,Double) = soh.getSoHDowAndUp(clusterParent._1,clusterChild._1)
      pw.println((i).formatted("%02d")+","+sohVal._1+","+sohVal._2)
    }
    pw.flush()
    pw.close()
  }
}


