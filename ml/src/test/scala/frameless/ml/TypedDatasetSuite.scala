package frameless.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalactic.anyvals.PosZInt
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait SparkTesting {
  val appID: String = new java.util.Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", appID)

  implicit def session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  implicit def sc: SparkContext = session.sparkContext
  implicit def sqlContext: SQLContext = session.sqlContext
}

class TypedDatasetSuite extends FunSuite with Checkers with SparkTesting {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(sizeRange = PosZInt(10), minSize = PosZInt(10))
}
