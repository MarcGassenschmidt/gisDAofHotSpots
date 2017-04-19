package clustering

import com.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by marc on 19.04.17.
  */
class Cluster {
  def test(sparkContext : SparkContext, file : String): Unit = {
    val monthFile = sparkContext.textFile(file)
    val withoutHeader : RDD[String] = dropHeader(monthFile)
    var counter = 0;
    val usedValues = monthFile.mapPartitions(lines => {
      val parser = new CSVParser(',')
      val map = lines.map(line => {
        val cols = line.split(",").map(_.trim)
        val result = new Row(counter, cols(5).toDouble, cols(6).toDouble)
        counter += 1
        result
      })

      map
    })
    val numberNodse = 4

    val latMin = usedValues.map(row => row.lat).min
    val lonMin = usedValues.map(row => row.lon).min
    val latMax = usedValues.map(row => row.lat).max
    val lonMax = usedValues.map(row => row.lon).max
    val partionSizeLat = (latMin+latMax)/Math.sqrt(numberNodse)
    val partionSizeLon = (lonMin+lonMax)/Math.sqrt(numberNodse)
    usedValues.filter(row => row.lat>4545)
    usedValues.filter(row => row.lat>4545)
    usedValues.filter(row => row.lat>4545)
    usedValues.filter(row => row.lat>4545)


  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(2)
      }
      lines
    })
  }
}
