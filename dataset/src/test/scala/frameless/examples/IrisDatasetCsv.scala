package frameless
package examples

import org.scalatest.Matchers

final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
final case class T(kind: String, sumSLen: Double, sumPLen: Double)

class IrisDatasetCsv extends TypedDatasetSuite with Matchers {
  private val testData: String = getClass.getResource("/iris.data").getPath
  test("read and count") {
    val data = TypedDataset.createUnsafe[Iris](session.read.csv(testData))
    data.count().run() shouldBe 150
  }

  test("test collecting class") {
    val data = TypedDataset.createUnsafe[Iris](session.read.csv(testData))
    import frameless.functions.aggregate._
    data.agg(collectSet(data('kind))).collect().run().flatten.toSet shouldBe
      Set("Iris-virginica", "Iris-setosa", "Iris-versicolor")
  }

  test("test join") {
    val data = TypedDataset.createUnsafe[Iris](session.read.csv(testData))
    import frameless.functions.aggregate._
    val ag = data.groupBy(data('kind)).agg(sum(data('sLength)), sum(data('pLength))).as[T]
    ag.joinFull(data)(ag('kind) === data('kind)).collect().run()
  }
}
