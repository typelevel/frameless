import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped
import TestHelpers._
import frameless._

class GroupByTests extends SpecWithContext {
  import testImplicits._
  
  def fooTF: TypedFrame[Foo] = Seq((1, "id1"), (4, "id3"), (5, "id2")).toDF.toTF

  test("count") {
    val toCount = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toCountTF: TypedFrame[Foo] = toCount.toDF.toTF
    
    val grouped: TypedFrame[Tuple1[Long]] = toCountTF.groupBy().count()
    checkAnswer(grouped, Seq(Tuple1(5l)))
    
    val groupedA: TypedFrame[(Int, Long)] = toCountTF.groupBy('a).count()
    checkAnswer(groupedA, Seq((1, 3l), (4, 1l), (5, 1l)))
    
    val groupedB: TypedFrame[(String, Long)] = toCountTF.groupBy('b).count()
    checkAnswer(groupedB, Set(("id1", 1l), ("id3", 3l), ("id2", 1l)))
    
    val groupedAB: TypedFrame[(Int, String, Long)] = toCountTF.groupBy('a, 'b).count()
    checkAnswer(groupedAB, Set((1, "id1", 1l),  (1, "id3", 2l), (4, "id3", 1l), (5, "id2", 1l)))
    
    illTyped("fooTF.groupBy('c)")
  }
  
  case class FooByte(a: Byte, b: String)
  case class FooShort(a: Short, b: String)
  case class FooInt(a: Int, b: String)
  case class FooLong(a: Long, b: String)
  case class FooFloat(a: Float, b: String)
  case class FooDouble(a: Double, b: String)
  
  def fByteTF: TypedFrame[FooByte] = Seq((1.toByte, "a"), (2.toByte, "a")).toDF.toTF
  def fShortTF: TypedFrame[FooShort] = Seq((1.toShort, "a"), (2.toShort, "a")).toDF.toTF
  def fIntTF: TypedFrame[FooInt] = Seq((1.toInt, "a"), (2.toInt, "a")).toDF.toTF
  def fLongTF: TypedFrame[FooLong] = Seq((1.toLong, "a"), (2.toLong, "a")).toDF.toTF
  def fFloatTF: TypedFrame[FooFloat] = Seq((1.toFloat, "a"), (2.toFloat, "a")).toDF.toTF
  def fDoubleTF: TypedFrame[FooDouble] = Seq((1.toDouble, "a"), (2.toDouble, "a")).toDF.toTF
  
  test("sum") {
    val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
    val toSumTF: TypedFrame[Foo] = toSum.toDF.toTF
    val summed: TypedFrame[(String, Long)] = toSumTF.groupBy('b).sum('a)
    checkAnswer(summed, Seq(("id1", 1l), ("id2", 5l), ("id3", 6l)))
    
    val sByte: TypedFrame[(String, Long)] = fByteTF.groupBy('b).sum('a)
    val sShort: TypedFrame[(String, Long)] = fShortTF.groupBy('b).sum('a)
    val sInt: TypedFrame[(String, Long)] = fIntTF.groupBy('b).sum('a)
    val sLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).sum('a)
    val sFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).sum('a)
    val sDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).sum('a)
    
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
    val toSumTF: TypedFrame[Foo] = toSum.toDF.toTF
    val avged: TypedFrame[(String, Double)] = toSumTF.groupBy('b).avg('a)
    checkAnswer(avged, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))
    
    val aByte: TypedFrame[(String, Double)] = fByteTF.groupBy('b).avg('a)
    val aShort: TypedFrame[(String, Double)] = fShortTF.groupBy('b).avg('a)
    val aInt: TypedFrame[(String, Double)] = fIntTF.groupBy('b).avg('a)
    val aLong: TypedFrame[(String, Double)] = fLongTF.groupBy('b).avg('a)
    val aFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).avg('a)
    val aDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).avg('a)
    
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
    val toSumTF: TypedFrame[Foo] = toSum.toDF.toTF
    val meaned: TypedFrame[(String, Double)] = toSumTF.groupBy('b).mean('a)
    checkAnswer(meaned, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))
    
    val mByte: TypedFrame[(String, Double)] = fByteTF.groupBy('b).mean('a)
    val mShort: TypedFrame[(String, Double)] = fShortTF.groupBy('b).mean('a)
    val mInt: TypedFrame[(String, Double)] = fIntTF.groupBy('b).mean('a)
    val mLong: TypedFrame[(String, Double)] = fLongTF.groupBy('b).mean('a)
    val mFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).mean('a)
    val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).mean('a)
    
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
    val toMaxTF: TypedFrame[Foo] = toMax.toDF.toTF
    val maxed: TypedFrame[(String, Int)] = toMaxTF.groupBy('b).max('a)
    checkAnswer(maxed, Seq(("id1", 1), ("id2", 5), ("id3", 4)))
    
    val mByte: TypedFrame[(String, Byte)] = fByteTF.groupBy('b).max('a)
    val mShort: TypedFrame[(String, Short)] = fShortTF.groupBy('b).max('a)
    val mInt: TypedFrame[(String, Int)] = fIntTF.groupBy('b).max('a)
    val mLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).max('a)
    val mFloat: TypedFrame[(String, Float)] = fFloatTF.groupBy('b).max('a)
    val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).max('a)
    
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
    val toMinTF: TypedFrame[Foo] = toMin.toDF.toTF
    val mined: TypedFrame[(String, Int)] = toMinTF.groupBy('b).min('a)
    checkAnswer(mined, Seq(("id1", 1), ("id2", 5), ("id3", 1)))
    
    val mByte: TypedFrame[(String, Byte)] = fByteTF.groupBy('b).min('a)
    val mShort: TypedFrame[(String, Short)] = fShortTF.groupBy('b).min('a)
    val mInt: TypedFrame[(String, Int)] = fIntTF.groupBy('b).min('a)
    val mLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).min('a)
    val mFloat: TypedFrame[(String, Float)] = fFloatTF.groupBy('b).min('a)
    val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).min('a)
    
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
