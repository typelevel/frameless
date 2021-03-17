package frameless
package examples

import org.apache.spark.sql.types.DoubleType

final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
final case class IrisLight(kind: String, sLength: Double)

class IrisDatasetCsv extends TypedDatasetSuite {
  private val testDataPathCsv: String = getClass.getResource("/iris.data").getPath
  test("read and count") {
    val testDataDf = session.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPathCsv)
    val data = TypedDataset.createUnsafe[Iris](testDataDf)
    assert(data.count().run() === 150)
  }

  test("test collecting class") {
    val testDataDf = session.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPathCsv)
    testDataDf.printSchema()
    val data = TypedDataset.createUnsafe[Iris](testDataDf)
    import frameless.functions.aggregate._
    assert(data.agg(collectSet(data('kind))).collect().run().flatten.toSet ===
      Set("Iris-virginica", "Iris-setosa", "Iris-versicolor"))
  }

//  test("fail when shcema is not defined") {
//    val testDataDf = session.read.format("csv").load(testDataPathCsv)
//    val data = TypedDataset.createUnsafe[Iris](testDataDf)
//    println(data.collect().run())
//  }

//  test("making the dat ") {
//    val testDataDf = session.read.format("csv").load(testDataPathCsv)
//    val data = TypedDataset.createUnsafe[Iris](testDataDf)
//    println(data.collect().run())
//  }

  test("fail when shcema is not defined 2") {
    val testDataDf = session.read.format("csv").load(testDataPathCsv)
    val projectedDf = testDataDf.select(testDataDf("_c4").as("kind"), testDataDf("_c1").cast(DoubleType).as("sLength"))
    val data = TypedDataset.createUnsafe[IrisLight](projectedDf)
    println(data.collect().run())
  }


//  private val testDataPathJson: String = getClass.getResource("/iris.json").getPath
//  test("test json") {
//    val testDataDf = session.read.format("json").load(testDataPathJson)
//    val data = TypedDataset.createUnsafe[Iris](testDataDf)
//    println(data.collect().run())
//  }
}
