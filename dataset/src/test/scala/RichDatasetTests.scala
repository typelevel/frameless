import org.apache.spark.sql.{SpecWithContext, Dataset}
import shapeless.test.illTyped
import frameless._

case class Foo(a: Int, b: String)
case class Bar(i: Int, s: String)
case class XXX(a: Int, b: Double)

case class Ls(i: Int, l: Long, f: Float, d: Double, s: String, b: Boolean)
case class Ts(d: Double, s: String, b: Boolean)
case class Fs(i: Double, j: Double, s: String, k: Double, t: String)

class RichDatasetTests extends SpecWithContext {
  import testImplicits._
  
  val fooTuples: Seq[(Int, String)] = Seq((1, "id1"), (4, "id3"), (5, "id2"))
  def fooDS: Dataset[Foo] = fooTuples.toDF.as[Foo]
  
  def checkAnswer[A](ds: Dataset[A], seq: Seq[A]): Unit =
    assert(ds.collect().toSeq == seq)
  
  test("typedAs") {
    fooDS.typedAs[Foo]()
    fooDS.typedAs[Bar]()
    illTyped("fooDS.typedAs[XXX]()")
  }
  
  test("sort") {
    val a: Dataset[Foo] = fooDS.sort('a)
    val ab: Dataset[Foo] = fooDS.sort('a, 'b)
    val ba: Dataset[Foo] = fooDS.sort('b, 'a)
    
    checkAnswer(a, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    checkAnswer(ab, Seq(Foo(1, "id1"), Foo(4, "id3"), Foo(5, "id2")))
    checkAnswer(ba, Seq(Foo(1, "id1"), Foo(5, "id2"), Foo(4, "id3")))
    
    illTyped("fooDS.sort()")
    illTyped("fooDS.sort('c)")
    illTyped("fooDS.sort('a, 'c)")
  }
  
  test("sortDesc") {
    val a: Dataset[Foo] = fooDS.sortDesc('a)
    val ab: Dataset[Foo] = fooDS.sortDesc('a, 'b)
    val ba: Dataset[Foo] = fooDS.sortDesc('b, 'a)
    
    checkAnswer(a, Seq(Foo(5, "id2"), Foo(4, "id3"), Foo(1, "id1")))
    checkAnswer(ab, Seq(Foo(5, "id2"), Foo(4, "id3"), Foo(1, "id1")))
    checkAnswer(ba, Seq(Foo(4, "id3"), Foo(5, "id2"), Foo(1, "id1")))
    
    illTyped("fooDS.sortDesc()")
    illTyped("fooDS.sortDesc('c)")
    illTyped("fooDS.sortDesc('a, 'c)")
  }
  
  test("selectColumns") {
    val a: Dataset[Tuple1[Int]] = fooDS.selectColumns('a)
    val ab: Dataset[(Int, String)] = fooDS.selectColumns('a, 'b)
    val ba: Dataset[(String, Int)] = fooDS.selectColumns('b, 'a)
    
    checkAnswer(a, Seq(Tuple1(1), Tuple1(4), Tuple1(5)))
    checkAnswer(ab, Seq((1, "id1"), (4, "id3"), (5, "id2")))
    checkAnswer(ba, Seq(("id1", 1), ("id3", 4), ("id2", 5)))
    
    illTyped("fooDS.selectColumns()")
    illTyped("fooDS.selectColumns('c)")
    illTyped("fooDS.selectColumns('a, 'c)")
  }
  
  test("dropColumns") {
    val dropA: Dataset[Tuple1[String]] = fooDS.dropColumns('a)
    checkAnswer(dropA, fooTuples.map(Tuple1 apply _._2))
    
    val dropB: Dataset[Tuple1[Int]] = fooDS.dropColumns('b)
    checkAnswer(dropB, fooTuples.map(Tuple1 apply _._1))
    
    illTyped("fooDS.dropColumns()")
    illTyped("fooDS.dropColumns('c)")
  }
  
  test("dropDuplicates") {
    def fooMe(s: Seq[(Int, String)]): Seq[Foo] = s.map(Function.tupled(Foo.apply))
    
    val withDupSeq = Seq((1, "a"), (1, "a"), (1, "b"), (2, "c"), (3, "c"))
    val withDup: Dataset[Foo] = withDupSeq.toDF.as[Foo]

    val allDup: Dataset[Foo] = withDup.dropDuplicates()
    val aDup: Dataset[Foo] = withDup.dropDuplicates('a)
    val bDup: Dataset[Foo] = withDup.dropDuplicates('b)
    val abDup: Dataset[Foo] = withDup.dropDuplicates('a, 'b)
    
    checkAnswer(allDup, fooMe(Seq((1, "a"), (1, "b"), (2, "c"), (3, "c"))))
    checkAnswer(aDup, fooMe(Seq((1, "a"), (2, "c"), (3, "c"))))
    checkAnswer(bDup, fooMe(Seq((1, "a"), (1, "b"), (2, "c"))))
    checkAnswer(abDup, (allDup.collect(): Seq[Foo]))
    
    illTyped("fooDS.dropDuplicates('c)")
  }
  
  test("replace") {
    val tsSeq = Seq((1d, "s", true), (3d, "c", true), (0d, "a", false))
    val ts: Dataset[Ts] = tsSeq.toDF.as[Ts]
    
    checkAnswer(
      ts.replace(Map(1d -> 2d, 3d -> 4d))('d),
      Seq(Ts(2d, "s", true), Ts(4d, "c", true), Ts(0d, "a", false)))
    
    checkAnswer(
      ts.replace(Map("s" -> "S", "c" -> "C", "a" -> "A"))('s),
      Seq(Ts(1d, "S", true), Ts(3d, "C", true), Ts(0d, "A", false)))
    
    illTyped("fooDS.replace(Map('c' -> 'd'))('c)")
    
    val fsSeq = Seq(
      (0d, 1.1d, "s", 0d, "s"),
      (0d, 0d, "c", 0d, "c"),
      (1.2d, 1.3d, "d", 1.4d, "d"))
    val fs: Dataset[Fs] = fsSeq.toDF.as[Fs]
    
    checkAnswer(
      fs.replace(Map(0d -> 1d))('i),
      Seq(
        Fs(1d, 1.1d, "s", 0d, "s"),
        Fs(1d, 0d, "c", 0d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.replace(Map(0d -> 1d))('i, 'j),
      Seq(
        Fs(1d, 1.1d, "s", 0d, "s"),
        Fs(1d, 1d, "c", 0d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.replace(Map(0d -> 1d))('k, 'i),
      Seq(
        Fs(1d, 1.1d, "s", 1d, "s"),
        Fs(1d, 0d, "c", 1d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.replace(Map("s" -> "S", "c" -> "C"))('s, 't),
      Seq(
        Fs(0d, 1.1d, "S", 0d, "S"),
        Fs(0d, 0d, "C", 0d, "C"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    illTyped("ts.replace(1)('i, 's)")
    illTyped("ts.replace(Map(0 -> 1))()")
  }
}
