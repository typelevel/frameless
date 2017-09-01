package frameless

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalactic.anyvals.PosZInt
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.prop.Checkers

trait SparkTesting { self: BeforeAndAfterAll =>

  val appID: String = new java.util.Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", appID)

  private var s: SparkSession = _

  implicit def session: SparkSession = s
  implicit def sc: SparkContext = session.sparkContext
  implicit def sqlContext: SQLContext = session.sqlContext

  override def beforeAll(): Unit = {
    assert(s == null)
    s = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterAll(): Unit = {
    if (s != null) {
      s.stop()
      s = null
    }
  }
}


class TypedDatasetSuite extends FunSuite with Checkers with BeforeAndAfterAll with SparkTesting {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(sizeRange = PosZInt(10), minSize = PosZInt(10))
}
