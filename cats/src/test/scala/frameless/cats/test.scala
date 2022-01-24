package frameless
package cats

import _root_.cats.Foldable
import _root_.cats.implicits._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext => SC}

import org.scalatest.compatible.Assertion
import org.scalactic.anyvals.PosInt
import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import Arbitrary._

import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

trait SparkTests {
  val appID: String = new java.util.Date().toString + math.floor(math.random() * 10E4).toLong.toString

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", appID)
    .set("spark.driver.host", "127.0.0.1")

  implicit def session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  implicit def sc: SparkContext = session.sparkContext

  implicit class seqToRdd[A: ClassTag](seq: Seq[A])(implicit sc: SC) {
    def toRdd: RDD[A] = sc.makeRDD(seq)
  }
}

object Tests {
  def innerPairwise(mx: Map[String, Int], my: Map[String, Int], check: (Any, Any) => Assertion)(implicit sc: SC): Assertion = {
    import frameless.cats.implicits._
    import frameless.cats.inner._
    val xs = sc.parallelize(mx.toSeq)
    val ys = sc.parallelize(my.toSeq)

    val mz0 = (xs |+| ys).collectAsMap()
    val mz1 = (xs join ys).mapValues { case (x, y) => x |+| y }.collectAsMap()
    val mz2 = (for { (k, x) <- mx; y <- my.get(k) } yield (k, x + y)).toMap
    check(mz0, mz1)
    check(mz1, mz2)

    val zs = sc.parallelize(mx.values.toSeq)
    check(xs.csumByKey.collectAsMap(), mx)
    check(zs.csum, zs.collect().sum)

    if (mx.nonEmpty) {
      check(xs.cminByKey.collectAsMap(), mx)
      check(xs.cmaxByKey.collectAsMap(), mx)
      check(zs.cmin, zs.collect().min)
      check(zs.cmax, zs.collect().max)
    } else check(1, 1)
  }
}

class Test extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks with SparkTests {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSize = PosInt(10))

  property("spark is working") {
    sc.parallelize(Seq(1, 2, 3)).collect() shouldBe Array(1,2,3)
  }

  property("inner pairwise monoid") {
    // Make sure we have non-empty map
    forAll { (xh: (String, Int), mx: Map[String, Int], yh: (String, Int), my: Map[String, Int]) =>
      Tests.innerPairwise(mx + xh, my + yh, _ shouldBe _)
    }
  }

  property("rdd simple numeric commutative semigroup") {
    import frameless.cats.implicits._

    forAll { seq: List[Int] =>
      val expectedSum = if (seq.isEmpty) None else Some(seq.sum)
      val expectedMin = if (seq.isEmpty) None else Some(seq.min)
      val expectedMax = if (seq.isEmpty) None else Some(seq.max)

      val rdd = seq.toRdd

      rdd.cmin shouldBe expectedMin.getOrElse(0)
      rdd.cminOption shouldBe expectedMin

      rdd.cmax shouldBe expectedMax.getOrElse(0)
      rdd.cmaxOption shouldBe expectedMax

      rdd.csum shouldBe expectedSum.getOrElse(0)
      rdd.csumOption shouldBe expectedSum
    }
  }

  property("rdd of SortedMap[Int,Int] commutative monoid") {
    import frameless.cats.implicits._
    forAll { seq: List[SortedMap[Int, Int]] =>
      val rdd = seq.toRdd
      rdd.csum shouldBe Foldable[List].fold(seq)
    }
  }

  property("rdd tuple commutative semigroup example") {
    import frameless.cats.implicits._
    forAll { seq: List[(Int, Int)] =>
      val expectedSum = if (seq.isEmpty) None else Some(Foldable[List].fold(seq))
      val rdd = seq.toRdd

      rdd.csum shouldBe expectedSum.getOrElse(0 -> 0)
      rdd.csumOption shouldBe expectedSum
    }
  }

  property("pair rdd numeric commutative semigroup example") {
    import frameless.cats.implicits._
    val seq = Seq( ("a",2), ("b",3), ("d",6), ("b",2), ("d",1) )
    val rdd = seq.toRdd
    rdd.cminByKey.collect().toSeq should contain theSameElementsAs Seq( ("a",2), ("b",2), ("d",1) )
    rdd.cmaxByKey.collect().toSeq should contain theSameElementsAs Seq( ("a",2), ("b",3), ("d",6) )
    rdd.csumByKey.collect().toSeq should contain theSameElementsAs Seq( ("a",2), ("b",5), ("d",7) )
  }
}
