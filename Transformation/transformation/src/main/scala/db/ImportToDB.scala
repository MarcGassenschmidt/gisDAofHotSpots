import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import java.io.PrintWriter

/**
  * Created by marc on 17.04.17.
  */
class ImportToDB {

  def editCSV(context: SparkContext): Unit = {

  }


  def editCSVatom(): Unit ={
    val bufferedSource = Source.fromFile("~/Downloads/xbl")
    val pw = new PrintWriter(new File("~/Downloads/xblOut.csv"))
    var counter = 0;
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      pw.println(counter.toString+","+cols(5)+","+cols(6)+","+cols(7)+","+cols(8)+",")
      counter += 1
    }
  }
}
