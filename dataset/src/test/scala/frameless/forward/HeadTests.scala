package frameless.forward

import frameless.{TypedDataset, TypedDatasetSuite, TypedEncoder, TypedExpressionEncoder, X1}
import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.reflect.ClassTag
import org.scalatest.matchers.should.Matchers

class HeadTests extends TypedDatasetSuite with Matchers {
  def propArray[A: TypedEncoder : ClassTag : Ordering](data: Vector[X1[A]])(implicit c: SparkSession): Prop = {
    import c.implicits._
    if(data.nonEmpty) {
      val tds = TypedDataset.
        create(c.createDataset(data)(
          TypedExpressionEncoder.apply[X1[A]]
        ).orderBy($"a".desc))
        (tds.headOption().run().get ?= data.max).
        &&(tds.head(1).run().head ?= data.max).
        &&(tds.head(4).run().toVector ?=
          data.sortBy(_.a)(implicitly[Ordering[A]].reverse).take(4))
    } else Prop.passed
  }

  test("headOption(), head(1), and head(4)") {
    check(propArray[Int] _)
    check(propArray[Char] _)
    check(propArray[String] _)
  }
}
