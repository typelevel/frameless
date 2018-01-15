package frameless
import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers

import scala.reflect.ClassTag

class TypedDatasetForwardedTests extends TypedDatasetSuite with Matchers {

  def propArray[A: TypedEncoder : ClassTag : Ordering](data: Vector[X1[A]])(implicit c: SparkSession): Prop = {
    import c.implicits._
    if(data.nonEmpty) {
      val tds = TypedDataset.
        create(c.createDataset(data)(
          TypedExpressionEncoder.apply[X1[A]]
        ).orderBy($"a".desc))
      (tds.first ?= data.max).
        &&(tds.head ?= data.max).
        &&(tds.head(1).head ?= data.max).
        &&(tds.head(4).toVector ?=
          data.sortBy(_.a)(implicitly[Ordering[A]].reverse).take(4))
    } else Prop.passed
  }

  test("first(), head(), head(1), and head(4)") {
    check(propArray[Int] _)
    check(propArray[Char] _)
    check(propArray[String] _)
  }
}
