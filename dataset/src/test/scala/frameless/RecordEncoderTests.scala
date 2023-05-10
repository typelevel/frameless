package frameless

import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  DecimalType,
  IntegerType,
  LongType,
  MapType,
  ObjectType,
  StringType,
  StructField,
  StructType
}

import shapeless.{HList, LabelledGeneric}
import shapeless.test.illTyped

import org.scalatest.matchers.should.Matchers

final class RecordEncoderTests extends TypedDatasetSuite with Matchers {
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

  test("Nesting with Seq") {
    import RecordEncoderTests._

    val obj = C(B(Seq(A(1))))
    val rdd = sc.parallelize(Seq(obj))
    val ds = session.createDataset(rdd)(TypedExpressionEncoder[C])

    ds.collect.head shouldBe obj
  }

  test("Nesting with Set") {
    import RecordEncoderTests._

    val obj = E(Set(B(Seq(A(1)))))
    val rdd = sc.parallelize(Seq(obj))
    val ds = session.createDataset(rdd)(TypedExpressionEncoder[E])

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

    val lorem = new Name("Lorem")

    safeDs.withColumnReplaced('name, functions.litValue(lorem)).
      collect.run() shouldBe expected.map(_.copy(name = lorem))
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

  test("Case class with simple Map") {
    import RecordEncoderTests._

    val encoder = TypedEncoder[D]

    encoder.jvmRepr shouldBe ObjectType(classOf[D])

    val expectedStructType = StructType(Seq(
      StructField("m", MapType(
        keyType = StringType,
        valueType = IntegerType,
        valueContainsNull = false), false)))

    encoder.catalystRepr shouldBe expectedStructType

    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val ds1 = TypedDataset.createUnsafe[D] {
      val df = Seq(
        """{"m":{"pizza":1,"sushi":2}}""",
        """{"m":{"red":3,"blue":4}}""",
      ).toDF

      df.withColumn(
        "jsonValue",
        F.from_json(df.col("value"), expectedStructType)).
        select("jsonValue.*")
    }

    val expected = Seq(
      D(m = Map("pizza" -> 1, "sushi" -> 2)),
      D(m = Map("red" -> 3, "blue" -> 4)))

    ds1.collect.run() shouldBe expected

    val m2 = Map("updated" -> 5)

    val ds2 = ds1.withColumnReplaced('m, functions.lit(m2))

    ds2.collect.run() shouldBe expected.map(_.copy(m = m2))
  }

  test("Case class with Map & Value class") {
    import RecordEncoderTests._

    val encoder = TypedEncoder[Student]

    encoder.jvmRepr shouldBe ObjectType(classOf[Student])

    val expectedStudentStructType = StructType(Seq(
      StructField("name", StringType, false),
      StructField("grades", MapType(
        keyType = StringType,
        valueType = DecimalType.SYSTEM_DEFAULT,
        valueContainsNull = false), false)))

    encoder.catalystRepr shouldBe expectedStudentStructType

    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val ds1 = TypedDataset.createUnsafe[Student] {
      val df = Seq(
        """{"name":"Foo","grades":{"math":1,"physics":"23.4"}}""",
        """{"name":"Bar","grades":{"biology":18.5,"geography":4}}""",
      ).toDF

      df.withColumn(
        "jsonValue",
        F.from_json(df.col("value"), expectedStudentStructType)).
        select("jsonValue.*")
    }

    val expected = Seq(
      Student(name = "Foo", grades = Map(
        new Subject("math") -> new Grade(BigDecimal(1)),
        new Subject("physics") -> new Grade(BigDecimal(23.4D)))),
      Student(name = "Bar", grades = Map(
        new Subject("biology") -> new Grade(BigDecimal(18.5)),
        new Subject("geography") -> new Grade(BigDecimal(4L)))))

    ds1.collect.run() shouldBe expected

    val grades = Map[Subject, Grade](
      new Subject("any") -> new Grade(BigDecimal(Long.MaxValue) + 1L))

    val ds2 = ds1.withColumnReplaced('grades, functions.lit(grades))

    ds2.collect.run() shouldBe Seq(
      Student("Foo", grades), Student("Bar", grades))
  }

  test("Encode binary array") {
    val encoder = TypedEncoder[Tuple2[String, Array[Byte]]]

    encoder.jvmRepr shouldBe ObjectType(
      classOf[Tuple2[String, Array[Byte]]])

    val expectedStructType = StructType(Seq(
      StructField("_1", StringType, false),
      StructField("_2", BinaryType, false)))

    encoder.catalystRepr shouldBe expectedStructType

    val ds1: TypedDataset[(String, Array[Byte])] = {
      val rdd = sc.parallelize(Seq(
        Row.fromTuple("Foo" -> Array[Byte](3, 4)),
        Row.fromTuple("Bar" -> Array[Byte](5))
      ))
      val df = session.createDataFrame(rdd, expectedStructType)

      TypedDataset.createUnsafe(df)(encoder)
    }

    val expected = Seq("Foo" -> Seq[Byte](3, 4), "Bar" -> Seq[Byte](5))

    ds1.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected

    val subjects = "lorem".getBytes("UTF-8").toSeq

    val ds2 = ds1.withColumnReplaced('_2, functions.lit(subjects.toArray))

    ds2.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected.map(_.copy(_2 = subjects))
  }

  test("Encode simple array") {
    val encoder = TypedEncoder[Tuple2[String, Array[Int]]]

    encoder.jvmRepr shouldBe ObjectType(
      classOf[Tuple2[String, Array[Int]]])

    val expectedStructType = StructType(Seq(
      StructField("_1", StringType, false),
      StructField("_2", ArrayType(IntegerType, false), false)))

    encoder.catalystRepr shouldBe expectedStructType

    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val ds1 = TypedDataset.createUnsafe[(String, Array[Int])] {
      val df = Seq(
        """{"_1":"Foo", "_2":[3, 4]}""",
        """{"_1":"Bar", "_2":[5]}""",
      ).toDF

      df.withColumn(
        "jsonValue",
        F.from_json(df.col("value"), expectedStructType)).
        select("jsonValue.*")
    }

    val expected = Seq("Foo" -> Seq(3, 4), "Bar" -> Seq(5))

    ds1.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected

    val subjects = Seq(6, 6, 7)

    val ds2 = ds1.withColumnReplaced('_2, functions.lit(subjects.toArray))

    ds2.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected.map(_.copy(_2 = subjects))
  }

  test("Encode array of Value class") {
    import RecordEncoderTests._

    val encoder = TypedEncoder[Tuple2[String, Array[Subject]]]

    encoder.jvmRepr shouldBe ObjectType(
      classOf[Tuple2[String, Array[Subject]]])

    val expectedStructType = StructType(Seq(
      StructField("_1", StringType, false),
      StructField("_2", ArrayType(StringType, false), false)))

    encoder.catalystRepr shouldBe expectedStructType

    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val ds1 = TypedDataset.createUnsafe[(String, Array[Subject])] {
      val df = Seq(
        """{"_1":"Foo", "_2":["math","physics"]}""",
        """{"_1":"Bar", "_2":["biology","geography"]}""",
      ).toDF

      df.withColumn(
        "jsonValue",
        F.from_json(df.col("value"), expectedStructType)).
        select("jsonValue.*")
    }

    val expected = Seq(
      "Foo" -> Seq(new Subject("math"), new Subject("physics")),
      "Bar" -> Seq(new Subject("biology"), new Subject("geography")))

    ds1.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected

    val subjects = Seq(new Subject("lorem"), new Subject("ipsum"))

    val ds2 = ds1.withColumnReplaced('_2, functions.lit(subjects.toArray))

    ds2.collect.run().map {
      case (_1, _2) => _1 -> _2.toSeq
    } shouldBe expected.map(_.copy(_2 = subjects))
  }

  test("Encode case class with simple Seq") {
    import RecordEncoderTests._

    val encoder = TypedEncoder[B]

    encoder.jvmRepr shouldBe ObjectType(classOf[B])

    val expectedStructType = StructType(Seq(
      StructField("a", ArrayType(StructType(Seq(
        StructField("x", IntegerType, false))), false), false)))

    encoder.catalystRepr shouldBe expectedStructType

    val ds1: TypedDataset[B] = {
      val rdd = sc.parallelize(Seq(
        Row.fromTuple(Tuple1(Seq(
          Row.fromTuple(Tuple1[Int](1)),
          Row.fromTuple(Tuple1[Int](3))
        ))),
        Row.fromTuple(Tuple1(Seq(
          Row.fromTuple(Tuple1[Int](2))
        )))
      ))
      val df = session.createDataFrame(rdd, expectedStructType)

      TypedDataset.createUnsafe(df)(encoder)
    }

    val expected = Seq(B(Seq(A(1), A(3))), B(Seq(A(2))))

    ds1.collect.run() shouldBe expected

    val as = Seq(A(5), A(6))

    val ds2 = ds1.withColumnReplaced('a, functions.lit(as))

    ds2.collect.run() shouldBe expected.map(_.copy(a = as))
  }

  test("Encode case class with Value class") {
    import RecordEncoderTests._

    val encoder = TypedEncoder[Tuple2[Int, Seq[Name]]]

    encoder.jvmRepr shouldBe ObjectType(classOf[Tuple2[Int, Seq[Name]]])

    val expectedStructType = StructType(Seq(
      StructField("_1", IntegerType, false),
      StructField("_2", ArrayType(StringType, false), false)))

    encoder.catalystRepr shouldBe expectedStructType

    val ds1 = TypedDataset.createUnsafe[(Int, Seq[Name])] {
      val sqlContext = session.sqlContext
      import sqlContext.implicits._

      val df = Seq(
        """{"_1":1, "_2":["foo", "bar"]}""",
        """{"_1":2, "_2":["lorem"]}""",
      ).toDF

      df.withColumn(
        "jsonValue",
        F.from_json(df.col("value"), expectedStructType)).
        select("jsonValue.*")
    }

    val expected = Seq(
      1 -> Seq(new Name("foo"), new Name("bar")),
      2 -> Seq(new Name("lorem")))

    ds1.collect.run() shouldBe expected
  }
}

// ---

case class UnitsOnly(a: Unit, b: Unit)

case class TupleWithUnits(
  u0: Unit, _1: Int, u1: Unit, u2: Unit, _2: String, u3: Unit)

object TupleWithUnits {
  def apply(_1: Int, _2: String): TupleWithUnits =
    TupleWithUnits((), _1, (), (), _2, ())
}

case class OptionalNesting(o: Option[TupleWithUnits])

object RecordEncoderTests {
  case class A(x: Int)
  case class B(a: Seq[A])
  case class C(b: B)

  class Name(val value: String) extends AnyVal with Serializable {
    override def toString = s"Name($value)"
  }

  case class Person(name: Name, age: Int)

  case class User(id: Long, name: Option[Name])

  case class D(m: Map[String, Int])
  case class E(b: Set[B])

  final class Subject(val name: String) extends AnyVal with Serializable

  final class Grade(val value: BigDecimal) extends AnyVal with Serializable

  case class Student(name: String, grades: Map[Subject, Grade])
}
