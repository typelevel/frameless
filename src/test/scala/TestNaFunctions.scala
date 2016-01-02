package typedframe

import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped

class TestNaFunctions extends SpecWithContext {
  import testImplicits._
  
  def fooTF: TypedFrame[Foo] = TypedFrame(Seq((1, "id1"), (4, "id3"), (5, "id2")).toDF)
  
  test("dropAny") {
    fooTF.na.dropAny(): TypedFrame[Foo]
    fooTF.na.dropAny('a): TypedFrame[Foo]
    fooTF.na.dropAny('b): TypedFrame[Foo]
    fooTF.na.dropAny('a, 'b): TypedFrame[Foo]
    illTyped("fooTF.na.dropAny('c)")
  }
  
  test("dropAll") {
    fooTF.na.dropAll(): TypedFrame[Foo]
    fooTF.na.dropAll('a): TypedFrame[Foo]
    fooTF.na.dropAll('b): TypedFrame[Foo]
    fooTF.na.dropAll('a, 'b): TypedFrame[Foo]
    illTyped("fooTF.na.dropAll('c)")
  }
  
  test("naDrop") {
    import eu.timepit.refined.auto._
    
    fooTF.na.drop(1)('a)
    fooTF.na.drop(1)('a, 'b)
    illTyped("fooTF.na.drop(0)('a)")
    illTyped("fooTF.na.drop(1)()")
    illTyped("fooTF.na.drop(1)('c)")
  }
  
  test("fill") {
    case class Ls(i: Int, l: Long, f: Float, d: Double, s: String, b: Boolean)
    val lsSeq = Seq((1, 1l, 1f, 1d, "s", true), (2, 2l, 2f, 2d, "t", false))
    val ls: TypedFrame[Ls] = TypedFrame(lsSeq.toDF)
    
    ls.na.fill(4)('i)
    ls.na.fill(4l)('l)
    ls.na.fill(4f)('f)
    ls.na.fill(4d)('d)
    ls.na.fill("A")('s)
    
    illTyped("ls.na.fill(3d)('s)")
    
    case class Fs(i: Int, j: Int, s: String, k: Int, t: String)
    val fsSeq = Seq((1, 1, "s", 10, "ss"), (2, 2, "t", 20, "tt"))
    val fs: TypedFrame[Fs] = TypedFrame(fsSeq.toDF)
    
    fs.na.fill(1)('i)
    fs.na.fill(1)('i, 'j)
    fs.na.fill(1)('k, 'i)
    fs.na.fill("string")('s, 't)
    
    illTyped("ls.na.fill(1)('i, 's)")
    illTyped("ls.na.fill(1)()")
  }
  
  test("replaceAll") {
    case class Ts(d: Double, s: String, b: Boolean)
    val tsSeq = Seq((1d, "s", true), (3d, "c", true), (0d, "a", false))
    val ts: TypedFrame[Ts] = TypedFrame(tsSeq.toDF)
    
    checkAnswer(
      ts.na.replaceAll(Map(1d -> 2d, 3d -> 4d)),
      Seq(Ts(2d, "s", true), Ts(4d, "c", true), Ts(0d, "a", false)))
    
    checkAnswer(
      ts.na.replaceAll(Map("s" -> "S", "c" -> "C", "a" -> "A")),
      Seq(Ts(1d, "S", true), Ts(3d, "C", true), Ts(0d, "A", false)))
    
    illTyped("ts.na.replaceAll(Map(true -> false))")
    illTyped("fooTF.na.replaceAll(Map(1d -> 2d))")
  }
  
  test("replace") {
    case class Ts(d: Double, s: String, b: Boolean)
    val tsSeq = Seq((1d, "s", true), (3d, "c", true), (0d, "a", false))
    val ts: TypedFrame[Ts] = TypedFrame(tsSeq.toDF)
    
    checkAnswer(
      ts.na.replace(Map(1d -> 2d, 3d -> 4d))('d),
      Seq(Ts(2d, "s", true), Ts(4d, "c", true), Ts(0d, "a", false)))
    
    checkAnswer(
      ts.na.replace(Map("s" -> "S", "c" -> "C", "a" -> "A"))('s),
      Seq(Ts(1d, "S", true), Ts(3d, "C", true), Ts(0d, "A", false)))
    
    illTyped("fooTF.na.replace(Map('c' -> 'd'))('c)")
    
    case class Fs(i: Double, j: Double, s: String, k: Double, t: String)
    val fsSeq = Seq(
      (0d, 1.1d, "s", 0d, "s"),
      (0d, 0d, "c", 0d, "c"),
      (1.2d, 1.3d, "d", 1.4d, "d"))
    val fs: TypedFrame[Fs] = TypedFrame(fsSeq.toDF)
    
    checkAnswer(
      fs.na.replace(Map(0d -> 1d))('i),
      Seq(
        Fs(1d, 1.1d, "s", 0d, "s"),
        Fs(1d, 0d, "c", 0d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.na.replace(Map(0d -> 1d))('i, 'j),
      Seq(
        Fs(1d, 1.1d, "s", 0d, "s"),
        Fs(1d, 1d, "c", 0d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.na.replace(Map(0d -> 1d))('k, 'i),
      Seq(
        Fs(1d, 1.1d, "s", 1d, "s"),
        Fs(1d, 0d, "c", 1d, "c"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    checkAnswer(
      fs.na.replace(Map("s" -> "S", "c" -> "C"))('s, 't),
      Seq(
        Fs(0d, 1.1d, "S", 0d, "S"),
        Fs(0d, 0d, "C", 0d, "C"),
        Fs(1.2d, 1.3d, "d", 1.4d, "d"))
    )
    
    illTyped("ts.na.replace(1)('i, 's)")
    illTyped("ts.na.replace(Map(0 -> 1))()")
  }
}
