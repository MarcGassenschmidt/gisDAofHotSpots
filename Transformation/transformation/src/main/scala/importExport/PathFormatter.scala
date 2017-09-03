package importExport

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import parmeters.Settings

/**
  * Created by marc on 05.06.17.
  */
object PathFormatter {
  def getResultDirectoryAndName(settings: Settings, resultType: ResultType.Value) : String = {
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    var sub = "GIS_Daten/"+settings.csvYear+"/"+settings.csvMonth+"/"+resultType+"/"
    if(settings.focal){
      sub += "focal/"
    } else {
      sub += "global/"
    }
    val dir = settings.ouptDirectory+"/"+sub
    val f = new File(dir)
    f.mkdirs()
    dir+"a"+settings.aggregationLevel+"_w"+settings.weightRadius+"_wT"+settings.weightRadiusTime+"_f"+settings.focalRange+"_fT"+settings.focalRangeTime+"result.txt"
  }


  def getDirectory(settings : Settings, extra : String): String ={
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    val formatter = DateTimeFormatter.ofPattern("dd_MM")
    var sub = settings.scenario+"/"+settings.csvYear+"/"+settings.csvMonth+"/"
    //var sub = "Time_"+LocalDateTime.now().format(formatter)+"/"
    if(extra.equals("raster")){
      //For alle settings equal
    } else if(settings.focal){
      sub += "focal/"+extra+"/FocalRange_"+settings.focalRange+"/"
    } else {
      sub += "global/"+extra+"/"
    }
    val dir = settings.ouptDirectory+settings.scenario+"/"+sub
    val f = new File(dir)
    
    f.mkdirs()
    dir
  }

  def getDirectoryAndName(settings : Settings, tifType: TifType.Value): String ={
    getDirectoryAndName(settings,tifType,true)
  }

  def getDirectoryAndName(settings : Settings, tifType: TifType.Value, isMultiband : Boolean): String ={
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    var sub = "GIS_Daten/Mulitband"+isMultiband+"/"+settings.csvYear+"/"+settings.csvMonth+"/"
    if(tifType==TifType.Raw){
      sub += "Raster/"
    } else if(settings.focal){
      sub += tifType+"/focal/"
    } else {
      sub += tifType+"/global/"
    }
    val dir = settings.ouptDirectory+"/"+sub
    val f = new File(dir)
    f.mkdirs()
    if(tifType==TifType.Raw){
      dir+"a"+settings.aggregationLevel+"_.tif"
    } else {
      return dir+"a"+settings.aggregationLevel+"_w"+settings.weightRadius+"_wT"+settings.weightRadiusTime+"_f"+settings.focalRange+"_fT"+settings.focalRangeTime+"_.tif"
    }

  }

  def exist(settings: Settings, tifType: TifType.Value): Boolean ={
    exist(settings,tifType,true)
  }

  def exist(settings: Settings, tifType: TifType.Value, isMultiband : Boolean): Boolean ={
    (new File(getDirectoryAndName(settings,tifType, isMultiband))).exists()
  }

}

object TifType extends Enumeration {
  val Raw,GStar,Cluster = Value
}

object ResultType extends Enumeration {
  val Validation,Metrik,Time,HotSpots = Value
}
