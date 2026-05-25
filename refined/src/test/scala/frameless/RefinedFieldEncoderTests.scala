package frameless

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  ObjectType,
  StringType,
  StructField,
  StructType
}

import org.scalatest.matchers.should.Matchers

class RefinedFieldEncoderTests extends TypedDatasetSuite with Matchers {
  test("Encode a bare refined type") {
    import eu.timepit.refined.auto._
    import eu.timepit.refined.types.string.NonEmptyString

    val encoder: TypedEncoder[NonEmptyString] = {
      import frameless.refined.refinedEncoder
      TypedEncoder[NonEmptyString]
    }

    val ss = session
    import ss.implicits._

    encoder.catalystRepr shouldBe StringType

    val nes: NonEmptyString = "Non Empty String"

    val unsafeDs =
      TypedDataset.createUnsafe(sc.parallelize(Seq(nes.value)).toDF())(encoder)

    val expected = Seq(nes)

    unsafeDs.collect().run() shouldBe expected
  }

  test("Encode case class with a refined field") {
    import RefinedTypesTests._

    // Check jvmRepr
    import org.apache.spark.sql.types.ObjectType

    encoderA.jvmRepr shouldBe ObjectType(classOf[A])

    // Check catalystRepr
    val expectedAStructType = StructType(
      Seq(
        StructField("a", IntegerType, false),
        StructField("s", StringType, false)
      )
    )

    encoderA.catalystRepr shouldBe expectedAStructType

    // Check unsafe
    val unsafeDs: TypedDataset[A] = {
      val rdd = sc.parallelize(Seq(Row(as.a, as.s.toString)))
      val df = session.createDataFrame(rdd, expectedAStructType)

      TypedDataset.createUnsafe(df)(encoderA)
    }

    val expected = Seq(as)

    unsafeDs.collect().run() shouldBe expected

    // Check safe
    val safeDs = TypedDataset.create(expected)

    safeDs.collect().run() shouldBe expected
  }

  test("Encode case class with a refined optional field") {
    import RefinedTypesTests._

    // Check jvmRepr
    encoderB.jvmRepr shouldBe ObjectType(classOf[B])

    // Check catalystRepr
    val expectedBStructType = StructType(
      Seq(
        StructField("a", IntegerType, false),
        StructField("s", StringType, true)
      )
    )

    encoderB.catalystRepr shouldBe expectedBStructType

    // Check unsafe
    val unsafeDs: TypedDataset[B] = {
      val rdd = sc.parallelize(
        Seq(
          Row(bs.a, bs.s.mkString),
          Row(2, null.asInstanceOf[String])
        )
      )

      val df = session.createDataFrame(rdd, expectedBStructType)

      TypedDataset.createUnsafe(df)(encoderB)
    }

    val expected = Seq(bs, B(2, None))

    unsafeDs.collect().run() shouldBe expected

    // Check safe
    val safeDs = TypedDataset.create(expected)

    safeDs.collect().run() shouldBe expected
  }
}

object RefinedTypesTests {
  import eu.timepit.refined.auto._
  import eu.timepit.refined.types.string.NonEmptyString

  case class A(a: Int, s: NonEmptyString)
  case class B(a: Int, s: Option[NonEmptyString])

  val nes: NonEmptyString = "Non Empty String"

  val as = A(-42, nes)
  val bs = B(-42, Option(nes))

  import frameless.refined._ // implicit instances for refined

  implicit val encoderA: TypedEncoder[A] = TypedEncoder.usingDerivation

  implicit val encoderB: TypedEncoder[B] = TypedEncoder.usingDerivation
}
