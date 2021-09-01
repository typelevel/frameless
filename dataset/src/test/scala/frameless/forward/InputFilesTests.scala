package frameless

import java.util.UUID

import org.apache.spark.sql.SparkSession

import org.scalatest.matchers.should.Matchers

import org.scalacheck.Prop
import org.scalacheck.Prop._

class InputFilesTests extends TypedDatasetSuite with Matchers {
  test("inputFiles") {

    def propText[A: TypedEncoder](data: Vector[A]): Prop = {
      val filePath = s"$TEST_OUTPUT_DIR/${UUID.randomUUID()}.txt"

      TypedDataset.create(data).dataset.write.text(filePath)
      val dataset =
        TypedDataset.create(implicitly[SparkSession].sparkContext.textFile(filePath))

      dataset.inputFiles sameElements dataset.dataset.inputFiles
    }

    def propCsv[A: TypedEncoder](data: Vector[A]): Prop = {
      val filePath = s"$TEST_OUTPUT_DIR/${UUID.randomUUID()}.csv"
      val inputDataset = TypedDataset.create(data)
      inputDataset.dataset.write.csv(filePath)

      val dataset = TypedDataset.createUnsafe(
        implicitly[SparkSession].sqlContext.read.schema(inputDataset.schema).csv(filePath))

      dataset.inputFiles sameElements dataset.dataset.inputFiles
    }

    def propJson[A: TypedEncoder](data: Vector[A]): Prop = {
      val filePath = s"$TEST_OUTPUT_DIR/${UUID.randomUUID()}.json"
      val inputDataset = TypedDataset.create(data)
      inputDataset.dataset.write.json(filePath)

      val dataset = TypedDataset.createUnsafe(
        implicitly[SparkSession].sqlContext.read.schema(inputDataset.schema).json(filePath))

      dataset.inputFiles sameElements dataset.dataset.inputFiles
    }

    check(forAll(propText[String] _))
    check(forAll(propCsv[String] _))
    check(forAll(propJson[String] _))
  }
}
