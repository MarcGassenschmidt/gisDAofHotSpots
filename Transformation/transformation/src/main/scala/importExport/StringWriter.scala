package importExport

import java.io.{File, PrintWriter}

import parmeters.Settings

/**
  * Created by marc on 26.07.17.
  */
object StringWriter {
  def writeFile(text : String, resultType: ResultType.Value, settings : Settings): Unit ={
    val pw = new PrintWriter(PathFormatter.getResultDirectoryAndName(settings, resultType))
    pw.write(text)
    pw.flush()
    pw.close()
  }

  def exists(resultType: ResultType.Value, settings : Settings): Boolean ={
    (new File(PathFormatter.getResultDirectoryAndName(settings, resultType))).exists()
  }


}
