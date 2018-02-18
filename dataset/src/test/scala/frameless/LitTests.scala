package frameless

import frameless.functions.lit

import org.scalacheck.Prop
import org.scalacheck.Prop._

class LitTests extends TypedDatasetSuite {
  def prop[A: TypedEncoder](value: A): Prop = {
    val df: TypedDataset[Int] = TypedDataset.create(1 :: Nil)

    // filter forces whole codegen
    val elems = df.deserialized.filter((_:Int) => true).select(lit(value))
      .collect()
      .run()
      .toVector

    // otherwise it uses local relation
    val localElems = df.select(lit(value))
      .collect()
      .run()
      .toVector


    (localElems ?= Vector(value)) && (elems ?= Vector(value))
  }

  test("select(lit(...))") {
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)

    check(prop[Option[Int]] _)
    check(prop[Option[String]] _)

    check(prop[Vector[Long]] _)
    check(prop[Vector[X1[Long]]] _)

    check(prop[Vector[String]] _)
    check(prop[Vector[X1[String]]] _)

    check(prop[X1[Int]] _)
    check(prop[X1[X1[Int]]] _)

    check(prop[Food] _)

    // doesn't work, object has to be serializable
    // check(prop[frameless.LocalDateTime] _)
  }

  test("#205: comparing literals encoded using Injection") {
    import org.apache.spark.sql.catalyst.util.DateTimeUtils
    implicit val dateAsInt: Injection[java.sql.Date, Int] =
      Injection(DateTimeUtils.fromJavaDate, DateTimeUtils.toJavaDate)

    val today = new java.sql.Date(System.currentTimeMillis)
    val data = Vector(P(42, today))
    val tds = TypedDataset.create(data)

    tds.filter(tds('d) === today).collect().run()
  }
}

final case class P(i: Int, d: java.sql.Date)
