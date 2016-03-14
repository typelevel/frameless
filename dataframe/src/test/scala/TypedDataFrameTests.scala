import org.apache.spark.sql.{SpecWithContext, DataFrame}
import org.scalatest.Matchers._
import shapeless.test.illTyped
import TestHelpers._
import frameless._

case class Foo(a: Int, b: String)

class TypedDataFrameTests extends SpecWithContext {
  import testImplicits._

  val fooTuples: Seq[(Int, String)] = Seq((1, "id1"), (4, "id3"), (5, "id2"))
  val fooSeq: Seq[Foo] = fooTuples.map(Foo.tupled)
  def fooTF: TypedDataFrame[Foo] = fooTuples.toDF.toTF

  test("as") {
    case class Bar(i: Int, j: String)
    case class C(j: String, i: Int)
    case class D(i: Int, j: String, k: Int)
    fooTF.as[Bar]()
    illTyped("fooTF.as[C]()")
    illTyped("fooTF.as[D]()")
  }

  test("sort") {
    val a: TypedDataFrame[Foo] = fooTF.sort('a)
    val ab: TypedDataFrame[Foo] = fooTF.sort('a, 'b)
    val ba: TypedDataFrame[Foo] = fooTF.sort('b, 'a)

    checkAnswer(a, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    checkAnswer(ab, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    checkAnswer(ba, Seq(Foo(1, "id1"), Foo(5, "id2"), Foo(4, "id3")))

    illTyped("fooTF.sort()")
    illTyped("fooTF.sort('c)")
    illTyped("fooTF.sort('a, 'c)")
  }

  test("sortDesc") {
    val a: TypedDataFrame[Foo] = fooTF.sortDesc('a)
    val ab: TypedDataFrame[Foo] = fooTF.sortDesc('a, 'b)
    val ba: TypedDataFrame[Foo] = fooTF.sortDesc('b, 'a)

    checkAnswer(a, Seq(Foo(5, "id2"), Foo(4, "id3"), Foo(1, "id1")))
    checkAnswer(ab, Seq(Foo(5, "id2"), Foo(4, "id3"), Foo(1, "id1")))
    checkAnswer(ba, Seq(Foo(4, "id3"), Foo(5, "id2"), Foo(1, "id1")))

    illTyped("fooTF.sortDesc()")
    illTyped("fooTF.sortDesc('c)")
    illTyped("fooTF.sortDesc('a, 'c)")
  }

  test("select") {
    val a: TypedDataFrame[Tuple1[Int]] = fooTF.select('a)
    val ab: TypedDataFrame[(Int, String)] = fooTF.select('a, 'b)
    val ba: TypedDataFrame[(String, Int)] = fooTF.select('b, 'a)

    checkAnswer(a, Seq(Tuple1(1), Tuple1(4), Tuple1(5)))
    checkAnswer(ab, Seq((1, "id1"), (4, "id3"), (5, "id2")))
    checkAnswer(ba, Seq(("id1", 1), ("id3", 4), ("id2", 5)))

    illTyped("fooTF.select()")
    illTyped("fooTF.select('c)")
    illTyped("fooTF.select('a, 'c)")
  }

  test("filter") {
    val gt: TypedDataFrame[Foo] = fooTF.filter(_.a > 2)
    val sw: TypedDataFrame[Foo] = fooTF.filter(_.b.endsWith("3"))

    checkAnswer(gt, Seq(Foo(4, "id3"), Foo(5, "id2")))
    checkAnswer(sw, Seq(Foo(4, "id3")))
  }

  test("limit") {
    val l0: TypedDataFrame[Foo] = fooTF.limit(0)
    val l1: TypedDataFrame[Foo] = fooTF.limit(1)
    val l2: TypedDataFrame[Foo] = fooTF.limit(2)
    val l100: TypedDataFrame[Foo] = fooTF.limit(100)

    checkAnswer(l0, Seq())
    checkAnswer(l1, Seq(Foo(1, "id1")))
    checkAnswer(l2, Seq(Foo(1, "id1"), Foo(4, "id3")))
    checkAnswer(l100, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
  }

  case class Bar(a: Int, s: String)
  val barTuples: Seq[(Int, String)] = Seq((1, "bar"), (4, "id3"), (5, "id2"))
  def barTF: TypedDataFrame[Bar] = barTuples.toDF.toTF

  case class Fuu(a: Int, i: Double)
  def fuuTF: TypedDataFrame[Fuu] = Seq((1, 1.1)).toDF.toTF

  test("unionAll") {
    val foofoo: TypedDataFrame[Foo] = fooTF.unionAll(fooTF)
    checkAnswer(foofoo, fooSeq ++ fooSeq)

    val foobar: TypedDataFrame[Foo] = fooTF.unionAll(barTF)
    checkAnswer(foobar, fooSeq ++ barTuples.map(Foo.tupled))

    illTyped("fooTF.unionAll(fuuTF)")
  }

  test("intersect") {
    val foofoo: TypedDataFrame[Foo] = fooTF.intersect(fooTF)
    checkAnswer(foofoo, fooSeq.intersect(fooSeq).toSet)

    val foobar: TypedDataFrame[Foo] = fooTF.intersect(barTF)
    checkAnswer(foobar, fooSeq.intersect(barTuples.map(Foo.tupled)).toSet)

    illTyped("fooTF.intersect(fuuTF)")
  }

  test("except") {
    val foofoo: TypedDataFrame[Foo] = fooTF.except(fooTF)
    checkAnswer(foofoo, Seq.empty[Foo])

    val foobar: TypedDataFrame[Foo] = fooTF.except(barTF)
    checkAnswer(foobar, fooSeq.filterNot(barTuples.map(Foo.tupled) contains _).toSet)

    illTyped("fooTF.except(fuuTF)")
  }

  test("explode") {
    val isss: TypedDataFrame[(Int, String, String, String)] =
      fooTF.explode { case Foo(a, b) => b.split("d").map(_ -> a.toString) }

    checkAnswer(isss, Set(
      (1, "id1", "i", "1"),
      (1, "id1", "1", "1"),
      (4, "id3", "i", "4"),
      (4, "id3", "3", "4"),
      (5, "id2", "i", "5"),
      (5, "id2", "2", "5")))

    val issb: TypedDataFrame[(Int, String, String, Boolean)] =
      fooTF.explode(f => List.fill(f.a)(f.b -> f.b.isEmpty))

    checkAnswer(issb, Set(
      (1, "id1", "id1", false),
      (1, "id1", "id1", false),
      (4, "id3", "id3", false),
      (4, "id3", "id3", false),
      (5, "id2", "id2", false),
      (5, "id2", "id2", false)))
  }

  test("drop") {
    val dropA: TypedDataFrame[Tuple1[String]] = fooTF.drop('a)
    checkAnswer(dropA, fooTuples.map(Tuple1 apply _._2))

    val dropB: TypedDataFrame[Tuple1[Int]] = fooTF.drop('b)
    checkAnswer(dropB, fooTuples.map(Tuple1 apply _._1))

    illTyped("fooTF.drop()")
    illTyped("fooTF.drop('c)")
  }

  test("dropDuplicates") {
    def fooTFMe(s: Seq[(Int, String)]): Seq[Foo] = s.map(Function.tupled(Foo.apply))

    val withDupSeq = Seq((1, "a"), (1, "a"), (1, "b"), (2, "c"), (3, "c"))
    val withDup: TypedDataFrame[Foo] = withDupSeq.toDF.toTF

    val allDup: TypedDataFrame[Foo] = withDup.dropDuplicates()
    val aDup: TypedDataFrame[Foo] = withDup.dropDuplicates('a)
    val bDup: TypedDataFrame[Foo] = withDup.dropDuplicates('b)
    val abDup: TypedDataFrame[Foo] = withDup.dropDuplicates('a, 'b)

    checkAnswer(allDup, fooTFMe(Seq((1, "a"), (1, "b"), (2, "c"), (3, "c"))))
    checkAnswer(aDup, fooTFMe(Seq((1, "a"), (2, "c"), (3, "c"))))
    checkAnswer(bDup, fooTFMe(Seq((1, "a"), (1, "b"), (2, "c"))))
    checkAnswer(abDup, (allDup.collect(): Seq[Foo]))

    illTyped("fooTF.dropDuplicates('c)")
  }

  test("describe") {
    fooTF.describe('a)
    fooTF.describe('b)
    fooTF.describe('a, 'b)
    illTyped("fooTF.describe()")
    illTyped("fooTF.describe('c)")
  }

  test("head") {
    fooTF.head() shouldBe fooSeq.head
  }

  test("take") {
    fooTF.take(0) shouldBe fooSeq.take(0)
    fooTF.take(1) shouldBe fooSeq.take(1)
    fooTF.take(2) shouldBe fooSeq.take(2)
  }

  test("reduce") {
    val function: (Foo, Foo) => Foo = (f1, f2) => Foo(f1.a + f2.a, "b")
    fooTF.reduce(function) shouldBe fooSeq.reduce(function)
  }

  test("map") {
    val function: Foo => (String, Int) = _.b -> 12
    checkAnswer(fooTF.map(function), fooSeq.map(function))
  }

  test("flatMap") {
    val function: Foo => List[(String, Int)] = f => List.fill(f.a)(f.b -> f.a)
    checkAnswer(fooTF.flatMap(function), fooSeq.flatMap(function))
  }

  test("mapPartitions") {
    val function: Foo => (String, Int) = _.b -> 12
    checkAnswer(fooTF.mapPartitions(_.map(function)), fooSeq.map(function))
  }

  test("collect") {
    fooTF.collect() shouldBe fooSeq
  }
}
