package cats
package bec

import cats.implicits._

import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import Arbitrary._
import org.scalatest._
import prop._

import scala.reflect.ClassTag
import org.apache.spark.{SparkConf, SparkContext => SC}
import org.apache.spark.rdd.RDD

trait SparkTests {

  implicit lazy val sc: SC =
    new SC(conf)

  lazy val conf: SparkConf =
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("cats.bec test")
}

object Tests {
  def innerPairwise(mx: Map[String, Int], my: Map[String, Int], check: (Any, Any) => Unit)(implicit sc: SC): Unit = {
    import frameless.cats.implicits._
    import frameless.cats.inner._
    val xs = sc.parallelize(mx.toSeq)
    val ys = sc.parallelize(my.toSeq)

    val mz0 = (xs |+| ys).collectAsMap
    val mz1 = (xs join ys).mapValues { case (x, y) => x |+| y }.collectAsMap
    val mz2 = (for { (k, x) <- mx; y <- my.get(k) } yield (k, x + y)).toMap
    check(mz0, mz1)
    check(mz1, mz2)

    val zs = sc.parallelize(mx.values.toSeq)
    check(xs.csumByKey.collectAsMap, mx)
    check(zs.csum, zs.collect.sum)

    if (mx.nonEmpty) {
      check(xs.cminByKey.collectAsMap, mx)
      check(xs.cmaxByKey.collectAsMap, mx)
      check(zs.cmin, zs.collect.min)
      check(zs.cmax, zs.collect.max)
    }
  }
}

class Test extends PropSpec with Matchers with PropertyChecks with SparkTests {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 10)

  property("spark is working") {
    sc.parallelize(Array(1, 2, 3)).collect shouldBe Array(1,2,3)
  }

  property("inner pairwise monoid") {
    // Make sure we have non-empty map
    forAll { (xh: (String, Int), mx: Map[String, Int], yh: (String, Int), my: Map[String, Int]) =>
      Tests.innerPairwise(mx + xh, my + yh, _ shouldBe _)
    }
  }
}
