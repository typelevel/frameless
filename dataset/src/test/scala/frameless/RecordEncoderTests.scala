package frameless

import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.sql.types.{
  IntegerType, LongType, ObjectType, StringType, StructField, StructType
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

  class Name(val value: String) extends AnyVal with Serializable {
    override def toString = value
  }

  case class Person(name: Name, age: Int)

  case class User(id: Long, name: Option[Name])
}

class RecordEncoderTests extends TypedDatasetSuite with Matchers {
  test("Unable to encode products made from units only") {
    illTyped("TypedEncoder[UnitsOnly]")
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
    import RecordEncoderTests._

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

  test("Case class with value class field") {
    import RecordEncoderTests._

    illTyped(
      // As `Person` is not a Value class
      "val _: RecordFieldEncoder[Person] = RecordFieldEncoder.valueClass")

    val fieldEncoder: RecordFieldEncoder[Name] = RecordFieldEncoder.valueClass

    fieldEncoder.encoder.catalystRepr shouldBe StringType
    fieldEncoder.encoder.jvmRepr shouldBe ObjectType(classOf[String])

    // Encode as a Person field
    val encoder = TypedEncoder[Person]

    encoder.jvmRepr shouldBe ObjectType(classOf[Person])

    val expectedPersonStructType = StructType(Seq(
      StructField("name", StringType, false),
      StructField("age", IntegerType, false)))

    encoder.catalystRepr shouldBe expectedPersonStructType

    val unsafeDs: TypedDataset[Person] = {
      val rdd = sc.parallelize(Seq(
        Row.fromTuple("Foo" -> 2),
        Row.fromTuple("Bar" -> 3)
      ))
      val df = session.createDataFrame(rdd, expectedPersonStructType)

      TypedDataset.createUnsafe(df)(encoder)
    }

    val expected = Seq(
      Person(new Name("Foo"), 2), Person(new Name("Bar"), 3))

    unsafeDs.collect.run() shouldBe expected

    // Safely created DS
    val safeDs = TypedDataset.create(expected)

    safeDs.collect.run() shouldBe expected

    // TODO: withColumnReplaced
  }

  test("Case class with value class as optional field") {
    import RecordEncoderTests._

    illTyped( // As `Person` is not a Value class
      """val _: RecordFieldEncoder[Option[Person]] =
           RecordFieldEncoder.optionValueClass""")

    val fieldEncoder: RecordFieldEncoder[Option[Name]] =
      RecordFieldEncoder.optionValueClass

    fieldEncoder.encoder.catalystRepr shouldBe StringType

    fieldEncoder.encoder. // !StringType
      jvmRepr shouldBe ObjectType(classOf[Option[_]])

    // Encode as a Person field
    val encoder = TypedEncoder[User]

    encoder.jvmRepr shouldBe ObjectType(classOf[User])

    val expectedPersonStructType = StructType(Seq(
      StructField("id", LongType, false),
      StructField("name", StringType, true)))

    encoder.catalystRepr shouldBe expectedPersonStructType

    val ds1: TypedDataset[User] = {
      val rdd = sc.parallelize(Seq(
        Row(1L, null),
        Row(2L, "Foo")
      ))

      val df = session.createDataFrame(rdd, expectedPersonStructType)

      TypedDataset.createUnsafe(df)(encoder)
    }

    ds1.collect.run() shouldBe Seq(
      User(1L, None),
      User(2L, Some(new Name("Foo"))))

    val ds2: TypedDataset[User] = {
      val sqlContext = session.sqlContext
      import sqlContext.implicits._

      val df1 = Seq(
        """{"id":3,"label":"unused"}""",
        """{"id":4,"name":"Lorem"}""",
        """{"id":5,"name":null}"""
      ).toDF

      val df2 = df1.withColumn(
        "jsonValue",
        F.from_json(df1.col("value"), expectedPersonStructType)).
        select("jsonValue.id", "jsonValue.name")

      TypedDataset.createUnsafe[User](df2)
    }

    val expected = Seq(
      User(3L, None),
      User(4L, Some(new Name("Lorem"))),
      User(5L, None))

    ds2.collect.run() shouldBe expected

    // Safely created ds
    TypedDataset.create(expected).collect.run() shouldBe expected
  }
}
