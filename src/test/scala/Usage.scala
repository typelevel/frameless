package typedframe

import eu.timepit.refined.auto._
import org.apache.spark.sql.{SpecWithContext, DataFrame}
import org.scalatest.Matchers._
import shapeless.test._

case class Foo(a: Int, b: String)

class Usage extends SpecWithContext {
  import testImplicits._
  
  val fooTuples: Seq[(Int, String)] = Seq((1, "id1"), (4, "id3"), (5, "id2"))
  val fooSeq: Seq[Foo] = fooTuples.map(Foo.tupled)
  def fooTF: TypedFrame[Foo] = TypedFrame(fooTuples.toDF)
    
  // test("as") {
  //   case class Bar(i: Int, j: String)
  //   case class C(j: String, i: Int)
  //   case class D(i: Int, j: String, k: Int)
  //   fooTF.as[Bar]()
  //   illTyped("fooTF.as[C]()")
  //   illTyped("fooTF.as[D]()")
  // }
  
  // test("cartesianJoin") {
  //   case class Bar(i: Double, j: String)
  //   val bar: TypedFrame[Bar] = TypedFrame(Seq((1.1, "s"), (2.2, "t")).toDF)
  //   val p = fooTF.cartesianJoin(bar): TypedFrame[(Int, String, Double, String)]
    
  //   checkAnswer(p, Set(
  //     (1, "id1", 1.1, "s"), (4, "id3", 1.1, "s"), (5, "id2", 1.1, "s"),
  //     (1, "id1", 2.2, "t"), (4, "id3", 2.2, "t"), (5, "id2", 2.2, "t")))
  // }
  
  case class Schema1(a: Int, b: Int, c: String)
  def s1: TypedFrame[Schema1] = TypedFrame(Seq((1, 10, "c"), (2, 20, "d"), (4, 40, "e")).toDF)
  
  def nullInstance[T] = null.asInstanceOf[T]
  
  test("joinUsing") {
    // val inner: TypedFrame[(Int, String, Int, String)] = fooTF.innerJoin(s1).using('a)
    // checkAnswer(inner, Set((1, "id1", 10, "c"), (4, "id3", 40, "e")))
    
    import shapeless._
    implicit val lol: Typeable[Any] = Typeable.anyTypeable
    
    val outer: TypedFrame[(Int, String, Int, String)] = fooTF.outerJoin(s1).using('a)
    
    println("outer.show()")
    outer.show()
    println("outer.collect()")
    println(outer.collect())
    
    checkAnswer(outer, Set(
      (1, "id1", 10, "c"),
      (2, null, 20, "d"),
      (4, "id3", 40, "e"),
      (5, "id2", null.asInstanceOf[Int], null)))
      
    
    // val leftOuter: TypedFrame[(Int, String, Int, String)] = fooTF.leftOuterJoin(s1).using('a)
    // val rightOuter: TypedFrame[(Int, String, Int, String)] = fooTF.rightOuterJoin(s1).using('a)
    // // TODO:
    // // fooTF.leftsemiJoin(s1).using('a): TypedFrame[(Int, String, Int, String)]
    
    // illTyped("fooTF.innerJoin(s1).using('b)")
    // illTyped("fooTF.innerJoin(s1).using('c)")
    
    // case class Schema2(a: Int, b: String, c: String)
    // val s2: TypedFrame[Schema2] = TypedFrame(Seq((1, "10", "c"), (2, "20", "d"), (4, "40", "e")).toDF)
    
    // // TODO: checkAnswer
    // fooTF.innerJoin(s2).using('a): TypedFrame[(Int, String, String, String)]
    // fooTF.innerJoin(s2).using('b): TypedFrame[(Int, String, Int, String)]
    // fooTF.innerJoin(s2).using('a, 'b): TypedFrame[(Int, String, String)]
    
    // illTyped("fooTF.innerJoin(s2).using('a, 'c)")
    // illTyped("fooTF.innerJoin(s2).using('c, 'b)")
  }
  
  // test("joinOn") {
  //   // TODO: checkAnswer
  //   fooTF.innerJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   fooTF.outerJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   fooTF.leftOuterJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   fooTF.rightOuterJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   // TODO:
  //   // fooTF.leftsemiJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
    
  //   case class Schema2(w: String, x: Int, y: Int, z: String)
  //   val s2: TypedFrame[Schema2] = TypedFrame(Seq(("10", 1, 10, "c"), ("20", 2, 20, "d"), ("40", 4, 40, "e")).toDF)
    
  //   // TODO: checkAnswer
  //   fooTF.innerJoin(s2).on('a).and('x)
  //   fooTF.innerJoin(s2).on('a).and('y)
  //   fooTF.innerJoin(s2).on('b).and('w)
  //   fooTF.innerJoin(s2).on('b).and('z)
  //   fooTF.innerJoin(s2).on('a, 'b).and('x, 'z)
  //   fooTF.innerJoin(s2).on('a, 'b).and('y, 'w)
    
  //   illTyped("fooTF.innerJoin(s2).on('a, 'b).and('z, 'x)")
  //   illTyped("fooTF.innerJoin(s2).on('a).and('w)")
  //   illTyped("fooTF.innerJoin(s2).on('x).and('a)")
  // }
  
  // test("orderBy") {
  //   val a: TypedFrame[Foo] = fooTF.orderBy('a)
  //   val ab: TypedFrame[Foo] = fooTF.orderBy('a, 'b)
  //   val ba: TypedFrame[Foo] = fooTF.orderBy('b, 'a)
    
  //   checkAnswer(a, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(ab, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(ba, Seq(Foo(1, "id1"), Foo(5, "id2"), Foo(4, "id3")))
    
  //   illTyped("fooTF.orderBy()")
  //   illTyped("fooTF.orderBy('c)")
  //   illTyped("fooTF.orderBy('a, 'c)")
  // }
  
  // test("select") {
  //   val a: TypedFrame[Tuple1[Int]] = fooTF.select('a)
  //   val ab: TypedFrame[(Int, String)] = fooTF.select('a, 'b)
  //   val ba: TypedFrame[(String, Int)] = fooTF.select('b, 'a)
    
  //   checkAnswer(a, Seq(Tuple1(1), Tuple1(4), Tuple1(5)))
  //   checkAnswer(ab, Seq((1, "id1"), (4, "id3"), (5, "id2")))
  //   checkAnswer(ba, Seq(("id1", 1), ("id3", 4), ("id2", 5)))
    
  //   illTyped("fooTF.select()")
  //   illTyped("fooTF.select('c)")
  //   illTyped("fooTF.select('a, 'c)")
  // }
  
  // test("filter") {
  //   val gt: TypedFrame[Foo] = fooTF.filter(_.a > 2)
  //   val sw: TypedFrame[Foo] = fooTF.filter(_.b.endsWith("3"))
    
  //   checkAnswer(gt, Seq(Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(sw, Seq(Foo(4, "id3")))
  // }
  
  // test("limit") {
  //   val l0: TypedFrame[Foo] = fooTF.limit(0)
  //   val l1: TypedFrame[Foo] = fooTF.limit(1)
  //   val l2: TypedFrame[Foo] = fooTF.limit(2)
  //   val l100: TypedFrame[Foo] = fooTF.limit(100)
    
  //   checkAnswer(l0, Seq())
  //   checkAnswer(l1, Seq(Foo(1, "id1")))
  //   checkAnswer(l2, Seq(Foo(1, "id1"), Foo(4, "id3")))
  //   checkAnswer(l100, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    
  //   illTyped("fooTF.limit(-1)")
  // }
  
  // case class Bar(a: Int, s: String)
  // val barTuples: Seq[(Int, String)] = Seq((1, "bar"), (4, "id3"), (5, "id2"))
  // def barTF: TypedFrame[Bar] = TypedFrame(barTuples.toDF)
  
  // case class Fuu(a: Int, i: Double)
  // def fuuTF: TypedFrame[Fuu] = TypedFrame(Seq((1, 1.1)).toDF)
  
  // test("unionAll") {
  //   val foofoo: TypedFrame[Foo] = fooTF.unionAll(fooTF)
  //   checkAnswer(foofoo, fooSeq ++ fooSeq)
    
  //   val foobar: TypedFrame[Foo] = fooTF.unionAll(barTF)
  //   checkAnswer(foobar, fooSeq ++ barTuples.map(Foo.tupled))
    
  //   illTyped("fooTF.unionAll(fuuTF)")
  // }
  
  // test("intersect") {
  //   val foofoo: TypedFrame[Foo] = fooTF.intersect(fooTF)
  //   checkAnswer(foofoo, fooSeq.intersect(fooSeq).toSet)
    
  //   val foobar: TypedFrame[Foo] = fooTF.intersect(barTF)
  //   checkAnswer(foobar, fooSeq.intersect(barTuples.map(Foo.tupled)).toSet)
    
  //   illTyped("fooTF.intersect(fuuTF)")
  // }
  
  // test("except") {
  //   val foofoo: TypedFrame[Foo] = fooTF.except(fooTF)
  //   checkAnswer(foofoo, Seq.empty[Foo])
    
  //   val foobar: TypedFrame[Foo] = fooTF.except(barTF)
  //   checkAnswer(foobar, fooSeq.filterNot(barTuples.map(Foo.tupled) contains _).toSet)
    
  //   illTyped("fooTF.except(fuuTF)")
  // }
  
  // test("sample") {
  //   // .0 needed, https://github.com/fthomas/refined/issues/96
  //   fooTF.sample(true, 0.0)
  //   fooTF.sample(false, 1.0)
  //   fooTF.sample(true, 0.5)
  //   illTyped("fooTF.sample(true, -0.1)")
  //   illTyped("fooTF.sample(true, 1.1)")
  // }
  
  // test("randomSplit") {
  //   fooTF.randomSplit(Array(0.1, 0.2, 0.7))
  //   illTyped("fooTF.randomSplit(Array(0.1, 0.2, -0.7))")
  // }
  
  // test("explode") {
  //   val isss: TypedFrame[(Int, String, String, String)] =
  //     fooTF.explode { case Foo(a, b) => b.split("d").map(_ -> a.toString) }
    
  //   checkAnswer(isss, Set(
  //     (1, "id1", "i", "1"),
  //     (1, "id1", "1", "1"),
  //     (4, "id3", "i", "4"),
  //     (4, "id3", "3", "4"),
  //     (5, "id2", "i", "5"),
  //     (5, "id2", "2", "5")
  //   ))
    
  //   val issb: TypedFrame[(Int, String, String, Boolean)] =
  //     fooTF.explode(f => List.fill(f.a)(f.b -> f.b.isEmpty))
    
  //   checkAnswer(issb, Set(
  //     (1, "id1", "id1", false),
  //     (1, "id1", "id1", false),
  //     (4, "id3", "id3", false),
  //     (4, "id3", "id3", false),
  //     (5, "id2", "id2", false),
  //     (5, "id2", "id2", false)
  //   ))
  // }
  
  // test("drop") {
  //   val dropA: TypedFrame[Tuple1[String]] = fooTF.drop('a)
  //   checkAnswer(dropA, fooTuples.map(Tuple1 apply _._2))
    
  //   val dropB: TypedFrame[Tuple1[Int]] = fooTF.drop('b)
  //   checkAnswer(dropB, fooTuples.map(Tuple1 apply _._1))
    
  //   illTyped("fooTF.drop()")
  //   illTyped("fooTF.drop('c)")
  // }
  
  // test("dropDuplicates") {
  //   def fooTFMe(s: Seq[(Int, String)]): Seq[Foo] =
  //     s.map(Function.tupled(Foo.apply))
    
  //   val withDupSeq = Seq((1, "a"), (1, "a"), (1, "b"), (2, "c"), (3, "c"))
  //   val withDup: TypedFrame[Foo] = TypedFrame(withDupSeq.toDF)

  //   val allDup: TypedFrame[Foo] = withDup.dropDuplicates()
  //   val aDup: TypedFrame[Foo] = withDup.dropDuplicates('a)
  //   val bDup: TypedFrame[Foo] = withDup.dropDuplicates('b)
  //   val abDup: TypedFrame[Foo] = withDup.dropDuplicates('a, 'b)
    
  //   checkAnswer(allDup, fooTFMe(Seq((1, "a"), (1, "b"), (2, "c"), (3, "c"))))
  //   checkAnswer(aDup, fooTFMe(Seq((1, "a"), (2, "c"), (3, "c"))))
  //   checkAnswer(bDup, fooTFMe(Seq((1, "a"), (1, "b"), (2, "c"))))
  //   checkAnswer(abDup, (allDup.collect(): Seq[Foo]))
    
  //   illTyped("fooTF.dropDuplicates('c)")
  // }
  
  // test("describe") {
  //   fooTF.describe('a)
  //   fooTF.describe('b)
  //   fooTF.describe('a, 'b)
  //   illTyped("fooTF.describe()")
  //   illTyped("fooTF.describe('c)")
  // }
  
  // test("repartition") {
  //   val r: TypedFrame[Foo] = fooTF.repartition(1)
  //   fooTF.collect() shouldBe r.collect()
  //   illTyped("fooTF.repartition(0)")
  //   illTyped("fooTF.repartition(-1)")
  // }
  
  // test("coalesce") {
  //   val c: TypedFrame[Foo] = fooTF.coalesce(1)
  //   fooTF.collect() shouldBe c.collect()
  //   illTyped("fooTF.coalesce(0)")
  //   illTyped("fooTF.coalesce(-1)")
  // }
  
  // test("show") {
  //   fooTF.show(1)
  //   illTyped("fooTF.show(0)")
  //   illTyped("fooTF.show(-1)")
  // }
  
  // test("groupBy.count") {
  //   val toCount = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toCountTF: TypedFrame[Foo] = TypedFrame(toCount.toDF)
    
  //   val grouped: TypedFrame[Tuple1[Long]] = toCountTF.groupBy().count()
  //   checkAnswer(grouped, Seq(Tuple1(5l)))
    
  //   val groupedA: TypedFrame[(Int, Long)] = toCountTF.groupBy('a).count()
  //   checkAnswer(groupedA, Seq((1, 3l), (4, 1l), (5, 1l)))
    
  //   val groupedB: TypedFrame[(String, Long)] = toCountTF.groupBy('b).count()
  //   checkAnswer(groupedB, Set(("id1", 1l), ("id3", 3l), ("id2", 1l)))
    
  //   val groupedAB: TypedFrame[(Int, String, Long)] = toCountTF.groupBy('a, 'b).count()
  //   checkAnswer(groupedAB, Set((1, "id1", 1l),  (1, "id3", 2l), (4, "id3", 1l), (5, "id2", 1l)))
    
  //   illTyped("fooTF.groupBy('c)")
  // }
  
  case class FooByte(a: Byte, b: String)
  case class FooShort(a: Short, b: String)
  case class FooInt(a: Int, b: String)
  case class FooLong(a: Long, b: String)
  case class FooFloat(a: Float, b: String)
  case class FooDouble(a: Double, b: String)
  
  def fByteTF: TypedFrame[FooByte] = TypedFrame(Seq((1.toByte, "a"), (2.toByte, "a")).toDF)
  def fShortTF: TypedFrame[FooShort] = TypedFrame(Seq((1.toShort, "a"), (2.toShort, "a")).toDF)
  def fIntTF: TypedFrame[FooInt] = TypedFrame(Seq((1.toInt, "a"), (2.toInt, "a")).toDF)
  def fLongTF: TypedFrame[FooLong] = TypedFrame(Seq((1.toLong, "a"), (2.toLong, "a")).toDF)
  def fFloatTF: TypedFrame[FooFloat] = TypedFrame(Seq((1.toFloat, "a"), (2.toFloat, "a")).toDF)
  def fDoubleTF: TypedFrame[FooDouble] = TypedFrame(Seq((1.toDouble, "a"), (2.toDouble, "a")).toDF)
  
  // test("groupBy.sum") {
  //   val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toSumTF: TypedFrame[Foo] = TypedFrame(toSum.toDF)
  //   val summed: TypedFrame[(String, Long)] = toSumTF.groupBy('b).sum('a)
  //   checkAnswer(summed, Seq(("id1", 1l), ("id2", 5l), ("id3", 6l)))
    
  //   val sByte: TypedFrame[(String, Long)] = fByteTF.groupBy('b).sum('a)
  //   val sShort: TypedFrame[(String, Long)] = fShortTF.groupBy('b).sum('a)
  //   val sInt: TypedFrame[(String, Long)] = fIntTF.groupBy('b).sum('a)
  //   val sLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).sum('a)
  //   val sFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).sum('a)
  //   val sDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).sum('a)
    
  //   checkAnswer(sByte, Seq(("a", 3.toLong)))
  //   checkAnswer(sShort, Seq(("a", 3.toLong)))
  //   checkAnswer(sInt, Seq(("a", 3.toLong)))
  //   checkAnswer(sLong, Seq(("a", 3.toLong)))
  //   checkAnswer(sFloat, Seq(("a", 3.toDouble)))
  //   checkAnswer(sDouble, Seq(("a", 3.toDouble)))
    
  //   illTyped("toSumTF.groupBy('b).sum('a, 'b)")
  //   illTyped("toSumTF.groupBy('b).sum('b)")
  //   illTyped("toSumTF.groupBy('b).sum()")
  // }
  
  // test("groupBy.avg") {
  //   val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toSumTF: TypedFrame[Foo] = TypedFrame(toSum.toDF)
  //   val avged: TypedFrame[(String, Double)] = toSumTF.groupBy('b).avg('a)
  //   checkAnswer(avged, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))
    
  //   val aByte: TypedFrame[(String, Double)] = fByteTF.groupBy('b).avg('a)
  //   val aShort: TypedFrame[(String, Double)] = fShortTF.groupBy('b).avg('a)
  //   val aInt: TypedFrame[(String, Double)] = fIntTF.groupBy('b).avg('a)
  //   val aLong: TypedFrame[(String, Double)] = fLongTF.groupBy('b).avg('a)
  //   val aFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).avg('a)
  //   val aDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).avg('a)
    
  //   checkAnswer(aByte, Seq(("a", 1.5d)))
  //   checkAnswer(aShort, Seq(("a", 1.5d)))
  //   checkAnswer(aInt, Seq(("a", 1.5d)))
  //   checkAnswer(aLong, Seq(("a", 1.5d)))
  //   checkAnswer(aFloat, Seq(("a", 1.5d)))
  //   checkAnswer(aDouble, Seq(("a", 1.5d)))
    
  //   illTyped("toSumTF.groupBy('b).avg('a, 'b)")
  //   illTyped("toSumTF.groupBy('b).avg('b)")
  //   illTyped("toSumTF.groupBy('b).avg()")
  // }

  // test("groupBy.mean") {
  //   val toSum = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toSumTF: TypedFrame[Foo] = TypedFrame(toSum.toDF)
  //   val meaned: TypedFrame[(String, Double)] = toSumTF.groupBy('b).mean('a)
  //   checkAnswer(meaned, Seq(("id1", 1d), ("id2", 5d), ("id3", 2d)))
    
  //   val mByte: TypedFrame[(String, Double)] = fByteTF.groupBy('b).mean('a)
  //   val mShort: TypedFrame[(String, Double)] = fShortTF.groupBy('b).mean('a)
  //   val mInt: TypedFrame[(String, Double)] = fIntTF.groupBy('b).mean('a)
  //   val mLong: TypedFrame[(String, Double)] = fLongTF.groupBy('b).mean('a)
  //   val mFloat: TypedFrame[(String, Double)] = fFloatTF.groupBy('b).mean('a)
  //   val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).mean('a)
    
  //   checkAnswer(mByte, Seq(("a", 1.5d)))
  //   checkAnswer(mShort, Seq(("a", 1.5d)))
  //   checkAnswer(mInt, Seq(("a", 1.5d)))
  //   checkAnswer(mLong, Seq(("a", 1.5d)))
  //   checkAnswer(mFloat, Seq(("a", 1.5d)))
  //   checkAnswer(mDouble, Seq(("a", 1.5d)))
    
  //   illTyped("toSumTF.groupBy('b).mean('a, 'b)")
  //   illTyped("toSumTF.groupBy('b).mean('b)")
  //   illTyped("toSumTF.groupBy('b).mean()")
  // }
  
  // test("groupBy.max") {
  //   val toMax = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toMaxTF: TypedFrame[Foo] = TypedFrame(toMax.toDF)
  //   val maxed: TypedFrame[(String, Int)] = toMaxTF.groupBy('b).max('a)
  //   checkAnswer(maxed, Seq(("id1", 1), ("id2", 5), ("id3", 4)))
    
  //   val mByte: TypedFrame[(String, Byte)] = fByteTF.groupBy('b).max('a)
  //   val mShort: TypedFrame[(String, Short)] = fShortTF.groupBy('b).max('a)
  //   val mInt: TypedFrame[(String, Int)] = fIntTF.groupBy('b).max('a)
  //   val mLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).max('a)
  //   val mFloat: TypedFrame[(String, Float)] = fFloatTF.groupBy('b).max('a)
  //   val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).max('a)
    
  //   checkAnswer(mByte, Seq(("a", 2.toByte)))
  //   checkAnswer(mShort, Seq(("a", 2.toShort)))
  //   checkAnswer(mInt, Seq(("a", 2.toInt)))
  //   checkAnswer(mLong, Seq(("a", 2.toLong)))
  //   checkAnswer(mFloat, Seq(("a", 2.toFloat)))
  //   checkAnswer(mDouble, Seq(("a", 2.toDouble)))
    
  //   illTyped("toSumTF.groupBy('b).max('a, 'b)")
  //   illTyped("toSumTF.groupBy('b).max('b)")
  //   illTyped("toSumTF.groupBy('b).max()")
  // }
  
  // test("groupBy.min") {
  //   val toMin = Seq((1, "id1"),  (1, "id3"), (1, "id3"), (4, "id3"), (5, "id2"))
  //   val toMinTF: TypedFrame[Foo] = TypedFrame(toMin.toDF)
  //   val mined: TypedFrame[(String, Int)] = toMinTF.groupBy('b).min('a)
  //   checkAnswer(mined, Seq(("id1", 1), ("id2", 5), ("id3", 1)))
    
  //   val mByte: TypedFrame[(String, Byte)] = fByteTF.groupBy('b).min('a)
  //   val mShort: TypedFrame[(String, Short)] = fShortTF.groupBy('b).min('a)
  //   val mInt: TypedFrame[(String, Int)] = fIntTF.groupBy('b).min('a)
  //   val mLong: TypedFrame[(String, Long)] = fLongTF.groupBy('b).min('a)
  //   val mFloat: TypedFrame[(String, Float)] = fFloatTF.groupBy('b).min('a)
  //   val mDouble: TypedFrame[(String, Double)] = fDoubleTF.groupBy('b).min('a)
    
  //   checkAnswer(mByte, Seq(("a", 1.toByte)))
  //   checkAnswer(mShort, Seq(("a", 1.toShort)))
  //   checkAnswer(mInt, Seq(("a", 1.toInt)))
  //   checkAnswer(mLong, Seq(("a", 1.toLong)))
  //   checkAnswer(mFloat, Seq(("a", 1.toFloat)))
  //   checkAnswer(mDouble, Seq(("a", 1.toDouble)))
    
  //   illTyped("toSumTF.groupBy('b).min('a, 'b)")
  //   illTyped("toSumTF.groupBy('b).min('b)")
  //   illTyped("toSumTF.groupBy('b).min()")
  // }
  
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
  
  // test("head") {
  //   fooTF.head() shouldBe fooSeq.head
  // }
  
  // test("take") {
  //   fooTF.take(0) shouldBe fooSeq.take(0)
  //   fooTF.take(1) shouldBe fooSeq.take(1)
  //   fooTF.take(2) shouldBe fooSeq.take(2)
  //   illTyped("fooTF.take(-1)")
  // }
  
  // test("reduce") {
  //   val function: (Foo, Foo) => Foo = (f1, f2) => Foo(f1.a + f2.a, "b")
  //   fooTF.reduce(function) shouldBe fooSeq.reduce(function)
  // }
  
  // test("map") {
  //   val function: Foo => (String, Int) = _.b -> 12
  //   checkAnswer(fooTF.map(function), fooSeq.map(function))
  // }
  
  // test("flatMap") {
  //   val function: Foo => List[(String, Int)] = f => List.fill(f.a)(f.b -> f.a)
  //   checkAnswer(fooTF.flatMap(function), fooSeq.flatMap(function))
  // }
  
  // test("mapPartitions") {
  //   val function: Foo => (String, Int) = _.b -> 12
  //   checkAnswer(fooTF.mapPartitions(_.map(function)), fooSeq.map(function))
  // }
  
  // test("foreach") {
  //   fooTF.foreach(f => println(f.a))
  // }
  
  // test("foreachPartition") {
  //   fooTF.foreachPartition(i => println(i.map(_.b).mkString(":")))
  // }
  
  // test("collect") {
  //   fooTF.collect() shouldBe fooSeq
  // }
  
  // test("na.dropAny") {
  //   fooTF.na.dropAny(): TypedFrame[Foo]
  //   fooTF.na.dropAny('a): TypedFrame[Foo]
  //   fooTF.na.dropAny('b): TypedFrame[Foo]
  //   fooTF.na.dropAny('a, 'b): TypedFrame[Foo]
  //   illTyped("fooTF.na.dropAny('c)")
  // }
  
  // test("na.dropAll") {
  //   fooTF.na.dropAll(): TypedFrame[Foo]
  //   fooTF.na.dropAll('a): TypedFrame[Foo]
  //   fooTF.na.dropAll('b): TypedFrame[Foo]
  //   fooTF.na.dropAll('a, 'b): TypedFrame[Foo]
  //   illTyped("fooTF.na.dropAll('c)")
  // }
  
  // test("na.naDrop") {
  //   fooTF.na.drop(1)('a)
  //   fooTF.na.drop(1)('a, 'b)
  //   illTyped("fooTF.na.drop(0)('a)")
  //   illTyped("fooTF.na.drop(1)()")
  //   illTyped("fooTF.na.drop(1)('c)")
  // }
  
  // test("na.fill") {
  //   case class Ts(i: Int, l: Long, f: Float, d: Double, s: String, b: Boolean)
  //   val tsSeq = Seq((1, 1l, 1f, 1d, "s", true), (2, 2l, 2f, 2d, "t", false))
  //   val ts: TypedFrame[Ts] = TypedFrame(tsSeq.toDF)
    
  //   ts.na.fill(4)('i)
  //   ts.na.fill(4l)('l)
  //   ts.na.fill(4f)('f)
  //   ts.na.fill(4d)('d)
  //   ts.na.fill("A")('s)
    
  //   illTyped("ts.na.fill(3d)('s)")
    
  //   case class Fs(i: Int, j: Int, s: String, k: Int, t: String)
  //   val fsSeq = Seq((1, 1, "s", 10, "ss"), (2, 2, "t", 20, "tt"))
  //   val fs: TypedFrame[Fs] = TypedFrame(fsSeq.toDF)
    
  //   fs.na.fill(1)('i)
  //   fs.na.fill(1)('i, 'j)
  //   fs.na.fill(1)('k, 'i)
  //   fs.na.fill("string")('s, 't)
    
  //   illTyped("ts.na.fill(1)('i, 's)")
  //   illTyped("ts.na.fill(1)()")
  // }
  
  // case class Ts(d: Double, s: String, b: Boolean)
  // val tsSeq = Seq((1d, "s", true), (3d, "c", true), (0d, "a", false))
  // def ts: TypedFrame[Ts] = TypedFrame(tsSeq.toDF)
  
  // test("na.replaceAll") {
  //   checkAnswer(
  //     ts.na.replaceAll(Map(1d -> 2d, 3d -> 4d)),
  //     Seq(Ts(2d, "s", true), Ts(4d, "c", true), Ts(0d, "a", false)))
    
  //   checkAnswer(
  //     ts.na.replaceAll(Map("s" -> "S", "c" -> "C", "a" -> "A")),
  //     Seq(Ts(1d, "S", true), Ts(3d, "C", true), Ts(0d, "A", false)))
    
  //   illTyped("ts.na.replaceAll(Map(true -> false))")
  //   illTyped("fooTF.na.replaceAll(Map(1d -> 2d))")
  // }
  
  // test("na.replace") {
  //   checkAnswer(
  //     ts.na.replace(Map(1d -> 2d, 3d -> 4d))('d),
  //     Seq(Ts(2d, "s", true), Ts(4d, "c", true), Ts(0d, "a", false)))
    
  //   checkAnswer(
  //     ts.na.replace(Map("s" -> "S", "c" -> "C", "a" -> "A"))('s),
  //     Seq(Ts(1d, "S", true), Ts(3d, "C", true), Ts(0d, "A", false)))
    
  //   illTyped("fooTF.na.replace(Map('c' -> 'd'))('c)")
    
  //   case class Fs(i: Double, j: Double, s: String, k: Double, t: String)
  //   val fsSeq = Seq(
  //     (0d, 1.1d, "s", 0d, "s"),
  //     (0d, 0d, "c", 0d, "c"),
  //     (1.2d, 1.3d, "d", 1.4d, "d"))
  //   val fs: TypedFrame[Fs] = TypedFrame(fsSeq.toDF)
    
  //   checkAnswer(
  //     fs.na.replace(Map(0d -> 1d))('i),
  //     Seq(
  //       Fs(1d, 1.1d, "s", 0d, "s"),
  //       Fs(1d, 0d, "c", 0d, "c"),
  //       Fs(1.2d, 1.3d, "d", 1.4d, "d"))
  //   )
    
  //   checkAnswer(
  //     fs.na.replace(Map(0d -> 1d))('i, 'j),
  //     Seq(
  //       Fs(1d, 1.1d, "s", 0d, "s"),
  //       Fs(1d, 1d, "c", 0d, "c"),
  //       Fs(1.2d, 1.3d, "d", 1.4d, "d"))
  //   )
    
  //   checkAnswer(
  //     fs.na.replace(Map(0d -> 1d))('k, 'i),
  //     Seq(
  //       Fs(1d, 1.1d, "s", 1d, "s"),
  //       Fs(1d, 0d, "c", 1d, "c"),
  //       Fs(1.2d, 1.3d, "d", 1.4d, "d"))
  //   )
    
  //   checkAnswer(
  //     fs.na.replace(Map("s" -> "S", "c" -> "C"))('s, 't),
  //     Seq(
  //       Fs(0d, 1.1d, "S", 0d, "S"),
  //       Fs(0d, 0d, "C", 0d, "C"),
  //       Fs(1.2d, 1.3d, "d", 1.4d, "d"))
  //   )
    
  //   illTyped("ts.na.replace(1)('i, 's)")
  //   illTyped("ts.na.replace(Map(0 -> 1))()")
  // }
  
  // case class Ns(i: Int, j: Int, d: Double, s: String)
  // val nsSeq = Seq((1, 2, 3.0, "s"), (2, 2, 8.0, "S"), (4, 4, 6.0, "c"))
  // def ns: TypedFrame[Ns] = TypedFrame(nsSeq.toDF)
  
  // test("stat.cov") {
  //   ns.stat.cov('i, 'j)
  //   ns.stat.cov('i, 'd)
  //   illTyped("ns.stat.cov('i, 's)")
  // }
  
  // test("stat.corr") {
  //   ns.stat.corr('i, 'j)
  //   ns.stat.corr('i, 'd)
  //   illTyped("ns.stat.corr('i, 's)")
  // }
  
  // test("stat.crosstab") {
  //   ns.stat.crosstab('i, 'j)
  //   ns.stat.crosstab('i, 'd)
  //   ns.stat.crosstab('i, 's)
  //   illTyped("ns.stat.corr('a, 's)")
  //   illTyped("ns.stat.corr('i, 'a)")
  // }
  
  // test("stat.freqItems") {
  //   fooTF.stat.freqItems()('a)
  //   fooTF.stat.freqItems(1.0)('b)
  //   fooTF.stat.freqItems(0.5)('a, 'b)
  //   illTyped("fooTF.stat.freqItems(0.5)()")
  //   illTyped("fooTF.stat.freqItems(-0.1)('a)")
  //   illTyped("fooTF.stat.freqItems(1.1)('a)")
  // }
  
  // test("stat.sampleBy") {
  //   fooTF.stat.sampleBy('a, Map(1 -> 0.5, -1 -> 1.0), 10l)
  //   fooTF.stat.sampleBy('b, Map("s" -> 0.0, "c" -> 0.5, "S" -> 0.1), 10l)
  //   illTyped("fooTF.stat.sampleBy('b, Map(1 -> 0.5), 10l)")
  //   illTyped("fooTF.stat.sampleBy('a, Map(1 -> 1.1), 10l)")
  // }
  
  // test("manyTuples") {
  //   type T = Tuple23[
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int]
    
  //   val tSeq = Seq(Tuple23(
  //     1, 2, 3, 4, 5, 6, 7, 8,
  //     9, 10, 11, 12, 13, 14, 15, 16,
  //     17, 18, 19, 20, 21, 22, 23))
    
  //   val tdf: TypedFrame[T] = TypedFrame(tSeq.toDF)
    
  //   val largeTypedFrame: TypedFrame[Tuple46[
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int]] =
  //       tdf.cartesianJoin(tdf)
    
  //   checkAnswer(
  //     largeTypedFrame,
  //     Seq(Tuple46(
  //       1, 2, 3, 4, 5, 6, 7, 8,
  //       9, 10, 11, 12, 13, 14, 15, 16,
  //       17, 18, 19, 20, 21, 22, 23,
  //       1, 2, 3, 4, 5, 6, 7, 8,
  //       9, 10, 11, 12, 13, 14, 15, 16,
  //       17, 18, 19, 20, 21, 22, 23))
  //   )
  // }
}
