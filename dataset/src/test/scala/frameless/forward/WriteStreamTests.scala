package frameless

import java.util.UUID

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.execution.streaming.MemoryStream

import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalacheck.Prop._

class WriteStreamTests extends TypedDatasetSuite {

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
    val spark = session
    import spark.implicits._
    def prop[A: TypedEncoder: Encoder](data: List[A]): Prop = {
      val uid = UUID.randomUUID()
      val uidNoHyphens = uid.toString.replace("-", "")
      val filePath = s"$TEST_OUTPUT_DIR/$uid}"
      val checkpointPath = s"$TEST_OUTPUT_DIR/checkpoint/$uid"
      val inputStream = MemoryStream[A]
      val input = TypedDataset.create(inputStream.toDS())
      val inputter = input
        .writeStream
        .format("csv")
        .option("checkpointLocation", s"$checkpointPath/input")
        .start(filePath)
      inputStream.addData(data)
      inputter.processAllAvailable()
      val dataset =
        TypedDataset.createUnsafe(sqlContext.readStream.schema(input.schema).csv(filePath))

      val tester = dataset
        .writeStream
        .option("checkpointLocation", s"$checkpointPath/tester")
        .format("memory")
        .queryName(s"testCsv_$uidNoHyphens")
        .start()
      tester.processAllAvailable()
      val output = spark.table(s"testCsv_$uidNoHyphens").as[A]
      TypedDataset.create(data).collect().run().groupBy(identity) ?= output
        .collect()
        .groupBy(identity)
        .map { case (k, arr) => (k, arr.toSeq) }
    }

    check(forAll(Gen.nonEmptyListOf(Gen.alphaNumStr.suchThat(_.nonEmpty)))(prop[String]))
    check(forAll(Gen.nonEmptyListOf(Arbitrary.arbitrary[Int]))(prop[Int]))
  }

  test("write parquet") {
    val spark = session
    import spark.implicits._
    def prop[A: TypedEncoder: Encoder](data: List[A]): Prop = {
      val uid = UUID.randomUUID()
      val uidNoHyphens = uid.toString.replace("-", "")
      val filePath = s"$TEST_OUTPUT_DIR/$uid}"
      val checkpointPath = s"$TEST_OUTPUT_DIR/checkpoint/$uid"
      val inputStream = MemoryStream[A]
      val input = TypedDataset.create(inputStream.toDS())
      val inputter = input
        .writeStream
        .format("parquet")
        .option("checkpointLocation", s"$checkpointPath/input")
        .start(filePath)
      inputStream.addData(data)
      inputter.processAllAvailable()
      val dataset =
        TypedDataset.createUnsafe(sqlContext.readStream.schema(input.schema).parquet(filePath))

      val tester = dataset
        .writeStream
        .option("checkpointLocation", s"$checkpointPath/tester")
        .format("memory")
        .queryName(s"testParquet_$uidNoHyphens")
        .start()
      tester.processAllAvailable()
      val output = spark.table(s"testParquet_$uidNoHyphens").as[A]
      TypedDataset.create(data).collect().run().groupBy(identity) ?= output
        .collect()
        .groupBy(identity)
        .map { case (k, arr) => (k, arr.toSeq) }
    }

    check(forAll(Gen.nonEmptyListOf(genWriteExample))(prop[WriteExample]))
  }
}
