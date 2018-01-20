package frameless

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop._
import org.scalacheck.{Gen, Prop}

class WriteTests extends TypedDatasetSuite {
  test("write") {
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

}