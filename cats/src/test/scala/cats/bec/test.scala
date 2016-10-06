package cats
package bec

import cats.implicits._
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import Arbitrary._
import org.apache.spark.rdd.RDD
import org.scalatest._
import prop._
import org.apache.spark.{SparkConf, SparkContext => SC}

trait SparkTests {

  implicit lazy val sc: SC =
    new SC(conf)

  lazy val conf: SparkConf =
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("cats.bec test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

  property("rdd simple numeric monoid example") {
    import frameless.cats.implicits._
    val theSeq = 1 to 20
    val toy = theSeq.toRdd
    toy.cmin shouldBe 1
    toy.cmax shouldBe 20
    toy.csum shouldBe theSeq.sum
  }

  property("rdd Map[Int,Int] monoid example - kryo") {
    import frameless.cats.implicits._
    import cats.implicits._
    val rdd: RDD[Map[Int, Int]] = 1.to(20).zip(1.to(20)).toRdd.map(Map(_))
    rdd.csum shouldBe 1.to(20).zip(1.to(20)).toMap
  }
}
