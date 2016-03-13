import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped
import TestHelpers._
import frameless._

class GroupByTests extends SpecWithContext {
  import testImplicits._

  def fooTF: TypedDataFrame[Foo] = Seq((1, "id1"), (4, "id3"), (5, "id2")).toDF.toTF

  test("count") {
    val toCount = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toCountTF: TypedDataFrame[Foo] = toCount.toDF.toTF

    val grouped: TypedDataFrame[Tuple1[Long]] = toCountTF.groupBy().count()
    checkAnswer(grouped, Seq(Tuple1(5l)))

    val groupedA: TypedDataFrame[(Int, Long)] = toCountTF.groupBy('a).count()
    checkAnswer(groupedA, Seq((1, 3l), (4, 1l), (5, 1l)))

    val groupedB: TypedDataFrame[(String, Long)] = toCountTF.groupBy('b).count()
    checkAnswer(groupedB, Set(("id1", 1l), ("id3", 3l), ("id2", 1l)))

    val groupedAB: TypedDataFrame[(Int, String, Long)] = toCountTF.groupBy('a, 'b).count()
    checkAnswer(groupedAB, Set((1, "id1", 1l),  (1, "id3", 2l), (4, "id3", 1l), (5, "id2", 1l)))

    illTyped("fooTF.groupBy('c)")
  }

  case class FooByte(a: Byte, b: String)
  case class FooShort(a: Short, b: String)
  case class FooInt(a: Int, b: String)
  case class FooLong(a: Long, b: String)
  case class FooFloat(a: Float, b: String)
  case class FooDouble(a: Double, b: String)

  def fByteTF: TypedDataFrame[FooByte] = Seq((1.toByte, "a"), (2.toByte, "a")).toDF.toTF
  def fShortTF: TypedDataFrame[FooShort] = Seq((1.toShort, "a"), (2.toShort, "a")).toDF.toTF
  def fIntTF: TypedDataFrame[FooInt] = Seq((1.toInt, "a"), (2.toInt, "a")).toDF.toTF
  def fLongTF: TypedDataFrame[FooLong] = Seq((1.toLong, "a"), (2.toLong, "a")).toDF.toTF
  def fFloatTF: TypedDataFrame[FooFloat] = Seq((1.toFloat, "a"), (2.toFloat, "a")).toDF.toTF
  def fDoubleTF: TypedDataFrame[FooDouble] = Seq((1.toDouble, "a"), (2.toDouble, "a")).toDF.toTF

  test("sum") {
    val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toSumTF: TypedDataFrame[Foo] = toSum.toDF.toTF
    val summed: TypedDataFrame[(String, Long)] = toSumTF.groupBy('b).sum('a)
    checkAnswer(summed, Seq(("id1", 1l), ("id2", 5l), ("id3", 6l)))

    val sByte: TypedDataFrame[(String, Long)] = fByteTF.groupBy('b).sum('a)
    val sShort: TypedDataFrame[(String, Long)] = fShortTF.groupBy('b).sum('a)
    val sInt: TypedDataFrame[(String, Long)] = fIntTF.groupBy('b).sum('a)
    val sLong: TypedDataFrame[(String, Long)] = fLongTF.groupBy('b).sum('a)
    val sFloat: TypedDataFrame[(String, Double)] = fFloatTF.groupBy('b).sum('a)
    val sDouble: TypedDataFrame[(String, Double)] = fDoubleTF.groupBy('b).sum('a)

    checkAnswer(sByte, Seq(("a", 3.toLong)))
    checkAnswer(sShort, Seq(("a", 3.toLong)))
    checkAnswer(sInt, Seq(("a", 3.toLong)))
    checkAnswer(sLong, Seq(("a", 3.toLong)))
    checkAnswer(sFloat, Seq(("a", 3.toDouble)))
    checkAnswer(sDouble, Seq(("a", 3.toDouble)))

    illTyped("toSumTF.groupBy('b).sum('a, 'b)")
    illTyped("toSumTF.groupBy('b).sum('b)")
    illTyped("toSumTF.groupBy('b).sum()")
  }

  test("avg") {
    val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toSumTF: TypedDataFrame[Foo] = toSum.toDF.toTF
    val avged: TypedDataFrame[(String, Double)] = toSumTF.groupBy('b).avg('a)
    checkAnswer(avged, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))

    val aByte: TypedDataFrame[(String, Double)] = fByteTF.groupBy('b).avg('a)
    val aShort: TypedDataFrame[(String, Double)] = fShortTF.groupBy('b).avg('a)
    val aInt: TypedDataFrame[(String, Double)] = fIntTF.groupBy('b).avg('a)
    val aLong: TypedDataFrame[(String, Double)] = fLongTF.groupBy('b).avg('a)
    val aFloat: TypedDataFrame[(String, Double)] = fFloatTF.groupBy('b).avg('a)
    val aDouble: TypedDataFrame[(String, Double)] = fDoubleTF.groupBy('b).avg('a)

    checkAnswer(aByte, Seq(("a", 1.5d)))
    checkAnswer(aShort, Seq(("a", 1.5d)))
    checkAnswer(aInt, Seq(("a", 1.5d)))
    checkAnswer(aLong, Seq(("a", 1.5d)))
    checkAnswer(aFloat, Seq(("a", 1.5d)))
    checkAnswer(aDouble, Seq(("a", 1.5d)))

    illTyped("toSumTF.groupBy('b).avg('a, 'b)")
    illTyped("toSumTF.groupBy('b).avg('b)")
    illTyped("toSumTF.groupBy('b).avg()")
  }

  test("mean") {
    val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toSumTF: TypedDataFrame[Foo] = toSum.toDF.toTF
    val meaned: TypedDataFrame[(String, Double)] = toSumTF.groupBy('b).mean('a)
    checkAnswer(meaned, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))

    val mByte: TypedDataFrame[(String, Double)] = fByteTF.groupBy('b).mean('a)
    val mShort: TypedDataFrame[(String, Double)] = fShortTF.groupBy('b).mean('a)
    val mInt: TypedDataFrame[(String, Double)] = fIntTF.groupBy('b).mean('a)
    val mLong: TypedDataFrame[(String, Double)] = fLongTF.groupBy('b).mean('a)
    val mFloat: TypedDataFrame[(String, Double)] = fFloatTF.groupBy('b).mean('a)
    val mDouble: TypedDataFrame[(String, Double)] = fDoubleTF.groupBy('b).mean('a)

    checkAnswer(mByte, Seq(("a", 1.5d)))
    checkAnswer(mShort, Seq(("a", 1.5d)))
    checkAnswer(mInt, Seq(("a", 1.5d)))
    checkAnswer(mLong, Seq(("a", 1.5d)))
    checkAnswer(mFloat, Seq(("a", 1.5d)))
    checkAnswer(mDouble, Seq(("a", 1.5d)))

    illTyped("toSumTF.groupBy('b).mean('a, 'b)")
    illTyped("toSumTF.groupBy('b).mean('b)")
    illTyped("toSumTF.groupBy('b).mean()")
  }

  test("max") {
    val toMax = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toMaxTF: TypedDataFrame[Foo] = toMax.toDF.toTF
    val maxed: TypedDataFrame[(String, Int)] = toMaxTF.groupBy('b).max('a)
    checkAnswer(maxed, Seq(("id1", 1), ("id2", 5), ("id3", 4)))

    val mByte: TypedDataFrame[(String, Byte)] = fByteTF.groupBy('b).max('a)
    val mShort: TypedDataFrame[(String, Short)] = fShortTF.groupBy('b).max('a)
    val mInt: TypedDataFrame[(String, Int)] = fIntTF.groupBy('b).max('a)
    val mLong: TypedDataFrame[(String, Long)] = fLongTF.groupBy('b).max('a)
    val mFloat: TypedDataFrame[(String, Float)] = fFloatTF.groupBy('b).max('a)
    val mDouble: TypedDataFrame[(String, Double)] = fDoubleTF.groupBy('b).max('a)

    checkAnswer(mByte, Seq(("a", 2.toByte)))
    checkAnswer(mShort, Seq(("a", 2.toShort)))
    checkAnswer(mInt, Seq(("a", 2.toInt)))
    checkAnswer(mLong, Seq(("a", 2.toLong)))
    checkAnswer(mFloat, Seq(("a", 2.toFloat)))
    checkAnswer(mDouble, Seq(("a", 2.toDouble)))

    illTyped("toSumTF.groupBy('b).max('a, 'b)")
    illTyped("toSumTF.groupBy('b).max('b)")
    illTyped("toSumTF.groupBy('b).max()")
  }

  test("min") {
    val toMin = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toMinTF: TypedDataFrame[Foo] = toMin.toDF.toTF
    val mined: TypedDataFrame[(String, Int)] = toMinTF.groupBy('b).min('a)
    checkAnswer(mined, Seq(("id1", 1), ("id2", 5), ("id3", 1)))

    val mByte: TypedDataFrame[(String, Byte)] = fByteTF.groupBy('b).min('a)
    val mShort: TypedDataFrame[(String, Short)] = fShortTF.groupBy('b).min('a)
    val mInt: TypedDataFrame[(String, Int)] = fIntTF.groupBy('b).min('a)
    val mLong: TypedDataFrame[(String, Long)] = fLongTF.groupBy('b).min('a)
    val mFloat: TypedDataFrame[(String, Float)] = fFloatTF.groupBy('b).min('a)
    val mDouble: TypedDataFrame[(String, Double)] = fDoubleTF.groupBy('b).min('a)

    checkAnswer(mByte, Seq(("a", 1.toByte)))
    checkAnswer(mShort, Seq(("a", 1.toShort)))
    checkAnswer(mInt, Seq(("a", 1.toInt)))
    checkAnswer(mLong, Seq(("a", 1.toLong)))
    checkAnswer(mFloat, Seq(("a", 1.toFloat)))
    checkAnswer(mDouble, Seq(("a", 1.toDouble)))

    illTyped("toSumTF.groupBy('b).min('a, 'b)")
    illTyped("toSumTF.groupBy('b).min('b)")
    illTyped("toSumTF.groupBy('b).min()")
  }

  // TODO
  // test("rollup") {
  //   fooTF.rollup('a)
  //   fooTF.rollup('b)
  //   fooTF.rollup('a, 'b)
  //   illTyped("fooTF.rollup('c)")
  // }

  // TODO
  // test("cube") {
  //   fooTF.cube('a)
  //   fooTF.cube('b)
  //   fooTF.cube('a, 'b)
  //   illTyped("fooTF.cube('c)")
  // }
}
