package frameless

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalactic.anyvals.PosZInt
import org.scalacheck.Arbitrary
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class TypedDatasetSuite extends FunSuite with Checkers with JavaTypeArbitraries {
  // Limit size of generated collections and number of checks because Travis
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(sizeRange = PosZInt(10), minSize = PosZInt(10))

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

trait JavaTypeArbitraries {
  import implicits.injections._

  implicit val arbitraryJavaBoolean =
    Arbitrary(Arbitrary.arbitrary[Boolean].map(Injection[java.lang.Boolean, Boolean].invert))
  implicit val arbitraryJavaInteger =
      Arbitrary(Arbitrary.arbitrary[Int].map(Injection[java.lang.Integer, Int].invert))
  implicit val arbitraryJavaLong =
      Arbitrary(Arbitrary.arbitrary[Long].map(Injection[java.lang.Long, Long].invert))
  implicit val arbitraryJavaDouble =
    Arbitrary(Arbitrary.arbitrary[Double].map(Injection[java.lang.Double, Double].invert))
  implicit val arbitraryJavaFloat =
    Arbitrary(Arbitrary.arbitrary[Float].map(Injection[java.lang.Float, Float].invert))
}

