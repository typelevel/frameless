package frameless

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  ObjectType, StringType, StructField, StructType
}
import shapeless.{HList, LabelledGeneric}
import shapeless.test.illTyped
import org.scalatest.matchers.should.Matchers

case class UnitsOnly(a: Unit, b: Unit)

case class TupleWithUnits(u0: Unit, _1: Int, u1: Unit, u2: Unit, _2: String, u3: Unit)

object TupleWithUnits {
  def apply(_1: Int, _2: String): TupleWithUnits = TupleWithUnits((), _1, (), (), _2, ())
}

case class OptionalNesting(o: Option[TupleWithUnits])

object RecordEncoderTests {
  case class A(x: Int)
  case class B(a: Seq[A])
  case class C(b: B)
}

class Name(val value: String) extends AnyVal

class RecordEncoderTests extends TypedDatasetSuite with Matchers {
  test("Unable to encode products made from units only") {
    illTyped("""TypedEncoder[UnitsOnly]""")
  }

  test("Dropping fields") {
    def dropUnitValues[L <: HList](l: L)(implicit d: DropUnitValues[L]): d.Out = d(l)
    val fields = LabelledGeneric[TupleWithUnits].to(TupleWithUnits(42, "something"))
    dropUnitValues(fields) shouldEqual LabelledGeneric[(Int, String)].to((42, "something"))
  }

  test("Representation skips units") {
    assert(TypedEncoder[(Int, String)].catalystRepr == TypedEncoder[TupleWithUnits].catalystRepr)
  }

  test("Serialization skips units") {
    val df = session.createDataFrame(Seq((1, "one"), (2, "two")))
    val ds = df.as[TupleWithUnits](TypedExpressionEncoder[TupleWithUnits])
    val tds = TypedDataset.create(Seq(TupleWithUnits(1, "one"), TupleWithUnits(2, "two")))

    df.collect shouldEqual tds.toDF.collect
    ds.collect.toSeq shouldEqual tds.collect.run
  }

  test("Empty nested record value becomes null on serialization") {
    val ds = TypedDataset.create(Seq(OptionalNesting(Option.empty)))
    val df = ds.toDF
    df.na.drop.count shouldBe 0
  }

  test("Empty nested record value becomes none on deserialization") {
    val rdd = sc.parallelize(Seq(Row(null)))
    val schema = TypedEncoder[OptionalNesting].catalystRepr.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, schema)
    val ds = TypedDataset.createUnsafe(df)(TypedEncoder[OptionalNesting])
    ds.firstOption.run.get.o.isEmpty shouldBe true
  }

  test("Deeply nested optional values have correct deserialization") {
    val rdd = sc.parallelize(Seq(Row(true, Row(null, null))))
    type NestedOptionPair = X2[Boolean, Option[X2[Option[Int], Option[String]]]]
    val schema = TypedEncoder[NestedOptionPair].catalystRepr.asInstanceOf[StructType]
    val df = session.createDataFrame(rdd, schema)
    val ds = TypedDataset.createUnsafe(df)(TypedEncoder[NestedOptionPair])
    ds.firstOption.run.get shouldBe X2(true, Some(X2(None, None)))
  }

  test("Nesting with collection") {
    import RecordEncoderTests._
    val obj = C(B(Seq(A(1))))
    val rdd = sc.parallelize(Seq(obj))
    val ds = session.createDataset(rdd)(TypedExpressionEncoder[C])
    ds.collect.head shouldBe obj
  }

  test("Scalar value class") {
    val encoder = TypedEncoder[Name]

    encoder.jvmRepr shouldBe ObjectType(classOf[Name])

    encoder.catalystRepr shouldBe StructType(
      Seq(StructField("value", StringType, false)))

    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    TypedDataset
      .createUnsafe[Name](Seq("Foo", "Bar").toDF)(encoder)
      .collect().run() shouldBe Seq(new Name("Foo"), new Name("Bar"))

  }
}
