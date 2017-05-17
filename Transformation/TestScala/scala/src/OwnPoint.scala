
/**
  * Created by marc on 13.04.17.
  */
class OwnPoint extends Table[(Int,String,Point)](tag, "test"){
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name")
  def geom = column[Point]("geom")
  //http://stackoverflow.com/questions/30132680/slick-scala-what-is-a-repbind-and-how-do-i-turn-it-into-a-value
  //def * = (id, name, geom)
}
