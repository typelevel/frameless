package frameless
package examples


final case class Iris(sLength: Double, sWidth: Double, pLength: Double, pWidth: Double, kind: String)
final case class T(kind: String, sumSLen: Double, sumPLen: Double)

case class Ai(d: Double, i: String, b: Int)
case class Bi(kind: String, c: Boolean)

class IrisDatasetCsv extends TypedDatasetSuite {
  private val testDataPath: String = getClass.getResource("/iris.data").getPath
  test("read and count") {
    val testDataDf = session.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
    val data = TypedDataset.createUnsafe[Iris](testDataDf)
    assert(data.count().run() === 150)
  }

  test("test collecting class") {
    val testDataDf = session.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
    val data = TypedDataset.createUnsafe[Iris](testDataDf)
    import frameless.functions.aggregate._
    assert(data.agg(collectSet(data('kind))).collect().run().flatten.toSet ===
      Set("Iris-virginica", "Iris-setosa", "Iris-versicolor"))
  }

  // Any type of join is problematic (full or inner same exception)
  // Just the aggregation with show.run works fine (without the join)
  // I removed the CSV from the equation, but materializing ag and using the materialized ag to do the join
  // I also made the data to be materialized to make sure that there is no CSV loading, still same error
  test("test join") {
    val testDataDf = session.read.format("csv").schema(TypedExpressionEncoder[Iris].schema).load(testDataPath)
    val data: TypedDataset[Iris] = TypedDataset.createUnsafe[Iris](testDataDf)
    import frameless.functions.aggregate._
    val ag = data.groupBy(data('kind)).agg(sum(data('sLength)), sum(data('pLength))).as[T]
    val summaryStats: TypedDataset[(T, Iris)] = ag.joinInner(data)(ag('kind) === data('kind))
    summaryStats.select(summaryStats.colMany('_1, 'kind)).show().run()
  }
}
