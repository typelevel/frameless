package typedframe

import eu.timepit.refined.auto._
import org.apache.spark.sql.{SpecWithContext, DataFrame}
import org.scalatest.Matchers._
import shapeless.test._

case class Foo(a: Int, b: String)

class Usage extends SpecWithContext with Serializable {
  import testImplicits._
  
  val fooSeq: Seq[(Int, String)] = Seq((1, "id1"), (4, "id3"), (5, "id2"))
  def foo: TypedFrame[Foo] = TypedFrame(fooSeq.toDF)
    
  // test("as") {
  //   case class Bar(i: Int, j: String)
  //   case class C(j: String, i: Int)
  //   case class D(i: Int, j: String, k: Int)
  //   foo.as[Bar]()
  //   illTyped("foo.as[C]()")
  //   illTyped("foo.as[D]()")
  // }
  
  // test("cartesianJoin") {
  //   case class Bar(i: Double, j: String)
  //   val bar: TypedFrame[Bar] = TypedFrame(Seq((1.1, "s"), (2.2, "t")).toDF)
  //   val p = foo.cartesianJoin(bar): TypedFrame[(Int, String, Double, String)]
    
  //   checkAnswer(p, Set(
  //     (1, "id1", 1.1, "s"), (4, "id3", 1.1, "s"), (5, "id2", 1.1, "s"),
  //     (1, "id1", 2.2, "t"), (4, "id3", 2.2, "t"), (5, "id2", 2.2, "t")))
  // }
  
  // case class Schema1(a: Int, b: Int, c: String)
  // def s1: TypedFrame[Schema1] = TypedFrame(Seq((1, 10, "c"), (2, 20, "d"), (4, 40, "e")).toDF)
  
  // test("joinUsing") {
  //   val inner: TypedFrame[(Int, String, Int, String)] = foo.innerJoin(s1).using('a)
  //   checkAnswer(inner, Set((1, "id1", 10, "c"), (4, "id3", 40, "e")))
    
  //   val outer: TypedFrame[(Int, String, Int, String)] = foo.outerJoin(s1).using('a)
  //   // TODO:
  //   // val l: Seq[(Int, String, Int, String)] = outer.collect
  //   // checkAnswer(outer, Set(
  //   //   (1, "id1", 10, "c"),
  //   //   (2, null, 20, "d"),
  //   //   (4, "id3", 40, "e"),
  //   //   (5, "id2", null.asInstanceOf[Int], null)))
  //   val leftOuter: TypedFrame[(Int, String, Int, String)] = foo.leftOuterJoin(s1).using('a)
  //   val rightOuter: TypedFrame[(Int, String, Int, String)] = foo.rightOuterJoin(s1).using('a)
  //   // TODO:
  //   // foo.leftsemiJoin(s1).using('a): TypedFrame[(Int, String, Int, String)]
    
  //   illTyped("foo.innerJoin(s1).using('b)")
  //   illTyped("foo.innerJoin(s1).using('c)")
    
  //   case class Schema2(a: Int, b: String, c: String)
  //   val s2: TypedFrame[Schema2] = TypedFrame(Seq((1, "10", "c"), (2, "20", "d"), (4, "40", "e")).toDF)
    
  //   // TODO: checkAnswer
  //   foo.innerJoin(s2).using('a): TypedFrame[(Int, String, String, String)]
  //   foo.innerJoin(s2).using('b): TypedFrame[(Int, String, Int, String)]
  //   foo.innerJoin(s2).using('a, 'b): TypedFrame[(Int, String, String)]
    
  //   illTyped("foo.innerJoin(s2).using('a, 'c)")
  //   illTyped("foo.innerJoin(s2).using('c, 'b)")
  // }
  
  // test("joinOn") {
  //   // TODO: checkAnswer
  //   foo.innerJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   foo.outerJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   foo.leftOuterJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   foo.rightOuterJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
  //   // TODO:
  //   // foo.leftsemiJoin(s1).on('a).and('a): TypedFrame[(Int, String, Int, Int, String)]
    
  //   case class Schema2(w: String, x: Int, y: Int, z: String)
  //   val s2: TypedFrame[Schema2] = TypedFrame(Seq(("10", 1, 10, "c"), ("20", 2, 20, "d"), ("40", 4, 40, "e")).toDF)
    
  //   // TODO: checkAnswer
  //   foo.innerJoin(s2).on('a).and('x)
  //   foo.innerJoin(s2).on('a).and('y)
  //   foo.innerJoin(s2).on('b).and('w)
  //   foo.innerJoin(s2).on('b).and('z)
  //   foo.innerJoin(s2).on('a, 'b).and('x, 'z)
  //   foo.innerJoin(s2).on('a, 'b).and('y, 'w)
    
  //   illTyped("foo.innerJoin(s2).on('a, 'b).and('z, 'x)")
  //   illTyped("foo.innerJoin(s2).on('a).and('w)")
  //   illTyped("foo.innerJoin(s2).on('x).and('a)")
  // }
  
  // test("orderBy") {
  //   val a: TypedFrame[Foo] = foo.orderBy('a)
  //   val ab: TypedFrame[Foo] = foo.orderBy('a, 'b)
  //   val ba: TypedFrame[Foo] = foo.orderBy('b, 'a)
    
  //   checkAnswer(a, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(ab, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(ba, Seq(Foo(1, "id1"), Foo(5, "id2"), Foo(4, "id3")))
    
  //   illTyped("foo.orderBy()")
  //   illTyped("foo.orderBy('c)")
  //   illTyped("foo.orderBy('a, 'c)")
  // }
  
  // test("select") {
  //   val a: TypedFrame[Tuple1[Int]] = foo.select('a)
  //   val ab: TypedFrame[(Int, String)] = foo.select('a, 'b)
  //   val ba: TypedFrame[(String, Int)] = foo.select('b, 'a)
    
  //   checkAnswer(a, Seq(Tuple1(1), Tuple1(4), Tuple1(5)))
  //   checkAnswer(ab, Seq((1, "id1"), (4, "id3"), (5, "id2")))
  //   checkAnswer(ba, Seq(("id1", 1), ("id3", 4), ("id2", 5)))
    
  //   illTyped("foo.select()")
  //   illTyped("foo.select('c)")
  //   illTyped("foo.select('a, 'c)")
  // }
  
  // TODO:
  // test("filter") {
  //   val gt: TypedFrame[Foo] = foo.filter(_.a > 2)
  //   val sw: TypedFrame[Foo] = foo.filter(_.b.endsWith("3"))
    
  //   checkAnswer(gt, Seq(Foo(4, "id3"), Foo(5, "id2")))
  //   checkAnswer(sw, Seq(Foo(4, "id3")))
  // }
  
  // test("limit") {
  //   val l0: TypedFrame[Foo] = foo.limit(0)
  //   val l1: TypedFrame[Foo] = foo.limit(1)
  //   val l2: TypedFrame[Foo] = foo.limit(2)
  //   val l100: TypedFrame[Foo] = foo.limit(100)
    
  //   checkAnswer(l0, Seq())
  //   checkAnswer(l1, Seq(Foo(1, "id1")))
  //   checkAnswer(l2, Seq(Foo(1, "id1"), Foo(4, "id3")))
  //   checkAnswer(l100, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    
  //   illTyped("foo.limit(-1)")
  // }
  
  // TODO: Sig is wrong, A U B => A =:= B
  // test("unionAll") {
  //   case class ABar(a: Int, i: Double)
  //   case class BBar(a: Int, b: String)
  //   case class CBar(u: Boolean, b: String, a: Int)
  //   case class DBar(u: Boolean)
    
  //   val aBar: TypedFrame[ABar] = TypedFrame(Seq((1, 1.1), (2, 2.2)).toDF)
  //   val bBar: TypedFrame[BBar] = TypedFrame(Seq((1, "11"), (2, "22")).toDF)
  //   val cBar: TypedFrame[CBar] = TypedFrame(Seq((true, "11", 1), (false, "22", 2)).toDF)
  //   val dBar: TypedFrame[DBar] = TypedFrame(Seq(Tuple1(false)).toDF)
  
  //   val fuf: TypedFrame[(Int, String)] = foo.unionAll(foo)
  //   // val fua: TypedFrame[(Int, String)] = foo.unionAll(aBar)
  //   val fub: TypedFrame[(Int, String)] = foo.unionAll(bBar)
  //   // val fuc: TypedFrame[(Int, String, Boolean)] = foo.unionAll(cBar)
  //   // val fud: TypedFrame[(Int, String, Boolean)] = foo.unionAll(dBar)
  // }
  
  // test("intersect") {
  //   case class ABar(a: Int, i: Double)
  //   case class BBar(a: Int, b: String, i: Double)
  //   case class CBar(u: Boolean, b: String, a: Int)
    
  //   val aBar: TypedFrame[ABar] = TypedFrame(df)
  //   val bBar: TypedFrame[BBar] = TypedFrame(df)
  //   val cBar: TypedFrame[CBar] = TypedFrame(df)
    
  //   foo.intersect(foo): TypedFrame[(Int, String)]
  //   foo.intersect(aBar): TypedFrame[Tuple1[Int]]
  //   foo.intersect(bBar): TypedFrame[(Int, String)]
  //   foo.intersect(cBar): TypedFrame[(Int, String)]
  // }
  
  // test("except") {
  //   case class ABar(a: Int, i: Double)
  //   case class DBar(u: Boolean)
    
  //   val aBar: TypedFrame[ABar] = TypedFrame(df)
  //   val dBar: TypedFrame[DBar] = TypedFrame(df)
    
  //   foo.except(aBar): TypedFrame[Tuple1[String]]
  //   foo.except(dBar): TypedFrame[(Int, String)]
  // }
  
  // test("sample") {
  //   // .0 needed, https://github.com/fthomas/refined/issues/96
  //   foo.sample(true, 0.0)
  //   foo.sample(false, 1.0)
  //   foo.sample(true, 0.5)
  //   illTyped("foo.sample(true, -0.1)")
  //   illTyped("foo.sample(true, 1.1)")
  // }
  
  // test("randomSplit") {
  //   foo.randomSplit(Array(0.1, 0.2, 0.7))
  //   illTyped("foo.randomSplit(Array(0.1, 0.2, -0.7))")
  // }
  
  // test("explode") {
  //   import scala.spores._
    
  //   val isss: TypedFrame[(Int, String, String, String)] =
  //     foo.explode { case Foo(a, b) => b.split("d").map(_ -> a.toString) }
    
  //   checkAnswer(isss, Set(
  //     (1, "id1", "i", "1"),
  //     (1, "id1", "1", "1"),
  //     (4, "id3", "i", "4"),
  //     (4, "id3", "3", "4"),
  //     (5, "id2", "i", "5"),
  //     (5, "id2", "2", "5")
  //   ))
    
  //   val issb: TypedFrame[(Int, String, String, Boolean)] =
  //     foo.explode(f => List.fill(f.a)(f.b -> f.b.isEmpty))
    
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
  //   val dropA: TypedFrame[Tuple1[String]] = foo.drop('a)
  //   checkAnswer(dropA, fooSeq.map(Tuple1 apply _._2))
    
  //   val dropB: TypedFrame[Tuple1[Int]] = foo.drop('b)
  //   checkAnswer(dropB, fooSeq.map(Tuple1 apply _._1))
    
  //   illTyped("foo.drop()")
  //   illTyped("foo.drop('c)")
  // }
  
  // test("dropDuplicates") {
  //   def fooMe(s: Seq[(Int, String)]): Seq[Foo] =
  //     s.map(Function.tupled(Foo.apply))
    
  //   val withDupSeq = Seq((1, "a"), (1, "a"), (1, "b"), (2, "c"), (3, "c"))
  //   val withDup: TypedFrame[Foo] = TypedFrame(withDupSeq.toDF)

  //   val allDup: TypedFrame[Foo] = withDup.dropDuplicates()
  //   val aDup: TypedFrame[Foo] = withDup.dropDuplicates('a)
  //   val bDup: TypedFrame[Foo] = withDup.dropDuplicates('b)
  //   val abDup: TypedFrame[Foo] = withDup.dropDuplicates('a, 'b)
    
  //   checkAnswer(allDup, fooMe(Seq((1, "a"), (1, "b"), (2, "c"), (3, "c"))))
  //   checkAnswer(aDup, fooMe(Seq((1, "a"), (2, "c"), (3, "c"))))
  //   checkAnswer(bDup, fooMe(Seq((1, "a"), (1, "b"), (2, "c"))))
  //   checkAnswer(abDup, (allDup.collect(): Seq[Foo]))
    
  //   illTyped("foo.dropDuplicates('c)")
  // }
  
  // test("describe") {
  //   foo.describe('a)
  //   foo.describe('b)
  //   foo.describe('a, 'b)
  //   illTyped("foo.describe()")
  //   illTyped("foo.describe('c)")
  // }
  
  // test("repartition") {
  //   val r: TypedFrame[Foo] = foo.repartition(1)
  //   foo.collect() shouldBe r.collect()
  //   illTyped("foo.repartition(0)")
  //   illTyped("foo.repartition(-1)")
  // }
  
  // test("coalesce") {
  //   val c: TypedFrame[Foo] = foo.coalesce(1)
  //   foo.collect() shouldBe c.collect()
  //   illTyped("foo.coalesce(0)")
  //   illTyped("foo.coalesce(-1)")
  // }
  
  // test("show") {
  //   foo.show(1)
  //   illTyped("foo.show(0)")
  //   illTyped("foo.show(-1)")
  // }
  
  // TODO
  // test("groupBy") {
  //   foo.groupBy('a)
  //   foo.groupBy('b)
  //   foo.groupBy('a, 'b)
  //   illTyped("foo.groupBy('c)")
  // }
  
  // TODO
  // test("rollup") {
  //   foo.rollup('a)
  //   foo.rollup('b)
  //   foo.rollup('a, 'b)
  //   illTyped("foo.rollup('c)")
  // }
  
  // TODO
  // test("cube") {
  //   foo.cube('a)
  //   foo.cube('b)
  //   foo.cube('a, 'b)
  //   illTyped("foo.cube('c)")
  // }
  
  test("head") {
    foo.head() shouldBe Foo(1, "id1")
  }
  
  test("take") {
    foo.take(0) shouldBe Seq.empty[Foo]
    foo.take(1) shouldBe Seq(foo.head())
    foo.take(2) shouldBe Seq(Foo(1, "id1"), Foo(4, "id3"))
    illTyped("foo.take(-1)")
  }
  
  // test("reduce") {
  //   foo.reduce({ case (f1, f2) => Foo(f1.a + f2.a, f1.b) }): Foo
  // }
  
  // test("map") {
  //   foo.map(_.b -> 12): TypedFrame[(String, Int)]
  // }
  
  // test("flatMap") {
  //   foo.flatMap(f => List.fill(f.a)(f.b -> f.b.isEmpty)): TypedFrame[(String, Boolean)]
  // }
  
  // test("mapPartitions") {
  //   foo.mapPartitions(_.map(_.b -> 12)): TypedFrame[(String, Int)]
  // }
  
  // test("foreach") {
  //   foo.foreach(f => println(f.a))
  // }
  
  // test("foreachPartition") {
  //   foo.foreachPartition(i => println(i.map(_.b).mkString(":")))
  // }
  
  // test("collect") {
  //   foo.collect(): Seq[Foo]
  //   ()
  // }
  
  // test("dropAny") {
  //   foo.na.dropAny(): TypedFrame[Foo]
  //   foo.na.dropAny('a): TypedFrame[Foo]
  //   foo.na.dropAny('b): TypedFrame[Foo]
  //   foo.na.dropAny('a, 'b): TypedFrame[Foo]
  //   illTyped("foo.na.dropAny('c)")
  // }
  
  // test("dropAll") {
  //   foo.na.dropAll(): TypedFrame[Foo]
  //   foo.na.dropAll('a): TypedFrame[Foo]
  //   foo.na.dropAll('b): TypedFrame[Foo]
  //   foo.na.dropAll('a, 'b): TypedFrame[Foo]
  //   illTyped("foo.na.dropAll('c)")
  // }
  
  // test("naDrop") {
  //   foo.na.drop(1)('a)
  //   foo.na.drop(1)('a, 'b)
  //   illTyped("foo.na.drop(0)('a)")
  //   illTyped("foo.na.drop(1)()")
  //   illTyped("foo.na.drop(1)('c)")
  // }
  
  // test("fill") {
  //   case class Ts(i: Int, l: Long, f: Float, d: Double, s: String, b: Boolean, c: Char)
  //   val ts: TypedFrame[Ts] = TypedFrame(df)
    
  //   ts.na.fill(1)('i)
  //   ts.na.fill(1l)('l)
  //   ts.na.fill(1f)('f)
  //   ts.na.fill(1d)('d)
  //   ts.na.fill("string")('s)
  //   ts.na.fill(false)('b)
    
  //   illTyped("ts.na.fill(false)('s)")
  //   illTyped("ts.na.fill('c')('c)")
    
  //   case class Fs(i: Int, j: Int, s: String, k: Int, t: String)
  //   val fs: TypedFrame[Fs] = TypedFrame(df)
    
  //   fs.na.fill(1)('i)
  //   fs.na.fill(1)('i, 'j)
  //   fs.na.fill(1)('k, 'i)
  //   fs.na.fill("string")('s, 't)
    
  //   illTyped("ts.na.fill(1)('i, 's)")
  //   illTyped("ts.na.fill(1)()")
  // }
  
  // test("replaceAll") {
  //   case class Ts(d: Double, s: String, b: Boolean, c: Char)
  //   val ts: TypedFrame[Ts] = TypedFrame(df)
    
  //   ts.na.replaceAll(Map(1d -> 2d, 3d -> 4d))
  //   ts.na.replaceAll(Map("s" -> "S", "c" -> "C", "a" -> "A"))
  //   ts.na.replaceAll(Map(true -> false))
    
  //   illTyped("foo.na.replaceAll(Map('c' -> 'd'))")
  //   illTyped("foo.na.replaceAll(Map(true -> false))")
  // }
  
  // test("replace") {
  //   case class Ts(d: Double, s: String, b: Boolean, c: Char)
  //   val ts: TypedFrame[Ts] = TypedFrame(df)
  
  //   ts.na.replace(Map(1d -> 2d, 3d -> 4d))('d)
  //   ts.na.replace(Map("s" -> "S", "c" -> "C", "a" -> "A"))('s)
  //   ts.na.replace(Map(true -> false))('b)
  //   illTyped("foo.na.replace(Map('c' -> 'd'))('c)")
    
  //   case class Fs(i: Double, j: Double, s: String, k: Double, t: String)
  //   val fs: TypedFrame[Fs] = TypedFrame(df)
    
  //   fs.na.replace(Map(0d -> 1d))('i)
  //   fs.na.replace(Map(0d -> 1d))('i, 'j)
  //   fs.na.replace(Map(0d -> 1d))('k, 'i)
  //   fs.na.replace(Map("s" -> "S", "c" -> "C"))('s, 't)
  //   illTyped("ts.na.replace(1)('i, 's)")
  //   illTyped("ts.na.replace(Map(0 -> 1))()")
  // }
  
  // test("cov") {
  //   case class Ns(i: Int, j: Int, d: Double, s: String)
  //   val ns: TypedFrame[Ns] = TypedFrame(df)
  //   ns.stat.cov('i, 'j)
  //   ns.stat.cov('i, 'd)
  //   illTyped("ns.stat.cov('i, 's)")
  // }
  
  // test("corr") {
  //   case class Ns(i: Int, j: Int, d: Double, s: String)
  //   val ns: TypedFrame[Ns] = TypedFrame(df)
  //   ns.stat.corr('i, 'j)
  //   ns.stat.corr('i, 'd)
  //   illTyped("ns.stat.corr('i, 's)")
  // }
  
  // test("crosstab") {
  //   case class Ns(i: Int, j: Int, d: Double, s: String)
  //   val ns: TypedFrame[Ns] = TypedFrame(df)
  //   ns.stat.crosstab('i, 'j)
  //   ns.stat.crosstab('i, 'd)
  //   ns.stat.crosstab('i, 's)
  // }
  
  // test("freqItems") {
  //   foo.stat.freqItems(0.0)('a)
  //   foo.stat.freqItems(1.0)('b)
  //   foo.stat.freqItems(0.5)('a, 'b)
  //   illTyped("foo.stat.freqItems(0.5)()")
  //   illTyped("foo.stat.freqItems(-0.1)('a)")
  //   illTyped("foo.stat.freqItems(1.1)('a)")
  // }
  
  // test("sampleBy") {
  //   foo.stat.sampleBy('a, Map(1 -> 0.5, -1 -> 1.0), 10l)
  //   foo.stat.sampleBy('b, Map("s" -> 0.0, "c" -> 0.5, "S" -> 0.1), 10l)
  //   illTyped("foo.stat.sampleBy('b, Map(1 -> 0.5), 10l)")
  //   illTyped("foo.stat.sampleBy('a, Map(1 -> 1.1), 10l)")
  // }
  
  // test("manyTuples") {
  //   type T = Tuple23[
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int]
    
  //   val t: T = Tuple23(
  //     1, 2, 3, 4, 5, 6, 7, 8,
  //     9, 10, 11, 12, 13, 14, 15, 16,
  //     17, 18, 19, 20, 21, 22, 23)
    
  //   val tdf: TypedFrame[T] = TypedFrame(df)
    
  //   tdf.cartesianJoin(tdf): TypedFrame[Tuple46[
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int, Int,
  //     Int, Int, Int, Int, Int, Int, Int]]
  // }
}
