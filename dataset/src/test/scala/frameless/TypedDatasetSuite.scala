package frameless

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.anyvals.PosZInt

class TypedDatasetSuite extends FunSuite with Checkers {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(sizeRange = PosZInt(10))

  val appID = new java.util.Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", appID)

  implicit def session = SparkSession.builder().config(conf).getOrCreate()
  implicit def sc = session.sparkContext
  implicit def sqlContext = session.sqlContext
}
