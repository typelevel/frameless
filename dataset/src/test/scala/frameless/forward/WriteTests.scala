package frameless

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop}

class WriteTests extends TypedDatasetSuite {

  val genNested = for {
    d <- Arbitrary.arbitrary[Double]
    as <- Arbitrary.arbitrary[String]
  } yield Nested(d, as)

  val genOptionFieldsOnly = for {
    o1 <- Gen.option(Arbitrary.arbitrary[Int])
    o2 <- Gen.option(genNested)
  } yield OptionFieldsOnly(o1, o2)

  val genWriteExample = for {
    i <- Arbitrary.arbitrary[Int]
    s <- Arbitrary.arbitrary[String]
    on <- Gen.option(genNested)
    ooo <- Gen.option(genOptionFieldsOnly)
  } yield WriteExample(i, s, on, ooo)

  test("write csv") {
    def prop[A: TypedEncoder](data: List[A]): Prop = {
      val filePath = s"$TEST_OUTPUT_DIR/${UUID.randomUUID()}"
      val input = TypedDataset.create(data)
      input.write.csv(filePath)

      val dataset = TypedDataset.createUnsafe(
        implicitly[SparkSession].sqlContext.read.schema(input.schema).csv(filePath))

      dataset.collect().run().groupBy(identity) ?= input.collect().run().groupBy(identity)
    }

    check(forAll(Gen.listOf(Gen.alphaNumStr.suchThat(_.nonEmpty)))(prop[String]))
    check(forAll(prop[Int] _))
  }

  test("write parquet") {
    def prop[A: TypedEncoder](data: List[A]): Prop = {

      val filePath = s"$TEST_OUTPUT_DIR/${UUID.randomUUID()}"
      val input = TypedDataset.create(data)
      input.write.parquet(filePath)

      val dataset = TypedDataset.createUnsafe(
        implicitly[SparkSession].sqlContext.read.schema(input.schema).parquet(filePath))

      dataset.collect().run().groupBy(identity) ?= input.collect().run().groupBy(identity)
    }
    check(forAll(Gen.listOf(genWriteExample))(prop[WriteExample]))

  }

}
case class Nested(i: Double, v: String)
case class OptionFieldsOnly(o1: Option[Int], o2: Option[Nested])
case class WriteExample(i: Int, s: String, on: Option[Nested], ooo: Option[OptionFieldsOnly])
