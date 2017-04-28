package demo
/**
  * Created by marc on 18.04.17.
  */
class TestCSV {

  //http://www.markhneedham.com/blog/2014/11/16/spark-parse-csv-file-and-group-by-column-value/
  def test(sparkContext : SparkContext, file : String): Unit = {

    val monthFile = sparkContext.textFile(file)
    val withoutHeader : RDD[String] = dropHeader(monthFile)
    monthFile.mapPartitions(lines => {
      val parser = new CSVParser(',')
      val map = lines.map(line => {
        parser.parseLine(line).mkString(",")
      })

      map
    })


  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

}
