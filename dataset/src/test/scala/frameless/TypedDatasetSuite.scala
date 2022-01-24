package frameless

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalactic.anyvals.PosZInt
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck.Prop
import org.scalacheck.Prop._
import scala.util.{Properties, Try}
import org.scalatest.funsuite.AnyFunSuite

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


class TypedDatasetSuite extends AnyFunSuite with Checkers with BeforeAndAfterAll with SparkTesting {
  // Limit size of generated collections and number of checks to avoid OutOfMemoryError
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = {
    def getPosZInt(name: String, default: PosZInt) = Properties.envOrNone(s"FRAMELESS_GEN_${name}")
      .flatMap(s => Try(s.toInt).toOption)
      .flatMap(PosZInt.from)
      .getOrElse(default)
    PropertyCheckConfiguration(
      sizeRange = getPosZInt("SIZE_RANGE", PosZInt(20)),
      minSize = getPosZInt("MIN_SIZE", PosZInt(0))
    )
  }

  implicit val sparkDelay: SparkDelay[Job] = Job.framelessSparkDelayForJob

  def approximatelyEqual[A](a: A, b: A)(implicit numeric: Numeric[A]): Prop = {
    val da = numeric.toDouble(a)
    val db = numeric.toDouble(b)
    val epsilon = 1E-6
    // Spark has a weird behaviour concerning expressions that should return Inf
    // Most of the time they return NaN instead, for instance stddev of Seq(-7.827553978923477E227, -5.009124275715786E153)
    if((da.isNaN || da.isInfinity) && (db.isNaN || db.isInfinity)) proved
    else if (
      (da - db).abs < epsilon ||
      (da - db).abs < da.abs / 100)
        proved
    else falsified :| s"Expected $a but got $b, which is more than 1% off and greater than epsilon = $epsilon."
  }
}
