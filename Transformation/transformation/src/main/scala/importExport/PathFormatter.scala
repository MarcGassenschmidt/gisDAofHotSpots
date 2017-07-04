package importExport

import java.io.File

import java.time.LocalDateTime
import parmeters.Settings

/**
  * Created by marc on 05.06.17.
  */
class PathFormatter {

  def getDirectory(settings : Settings, extra : String): String ={
    if(settings.test){
      settings.ouptDirectory = "/tmp/"
    }
    var sub = "Time_"+LocalDateTime.now().formatted("dd_MM")+"/"
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

}
