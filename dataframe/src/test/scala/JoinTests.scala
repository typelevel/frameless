import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped
import TestHelpers._
import frameless._

class JoinTests extends SpecWithContext {
  import testImplicits._

  def fooTF: TypedDataFrame[Foo] = Seq((1, "id1"), (4, "id3"), (5, "id2")).toDF.toTF

  case class Schema1(a: Int, b: Int, c: String)
  def s1: TypedDataFrame[Schema1] = Seq((1, 10, "c"), (2, 20, "d"), (4, 40, "e")).toDF.toTF

  test("cartesianJoin") {
    val cartesian: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.cartesianJoin(s1)
    checkAnswer(cartesian, Set(
      (1, "id1", 1, 10, "c"),
      (1, "id1", 2, 20, "d"),
      (1, "id1", 4, 40, "e"),
      (4, "id3", 1, 10, "c"),
      (4, "id3", 2, 20, "d"),
      (4, "id3", 4, 40, "e"),
      (5, "id2", 1, 10, "c"),
      (5, "id2", 2, 20, "d"),
      (5, "id2", 4, 40, "e")))
  }

  test("innerJoin using") {
    val inner: TypedDataFrame[(Int, String, Int, String)] = fooTF.innerJoin(s1).using('a)
    checkAnswer(inner, Set(
      (1, "id1", 10, "c"),
      (4, "id3", 40, "e")))

    illTyped("fooTF.innerJoin(s1).using('b)")
    illTyped("fooTF.innerJoin(s1).using('c)")
    illTyped("fooTF.innerJoin(s1).using('a, 'b)")
  }

  test("outerJoin using") {
    val outer: TypedDataFrame[(Int, String, Int, String)] = fooTF.outerJoin(s1).using('a)
    checkAnswer(outer, Set(
      (1, "id1", 10, "c"),
      (2, null, 20, "d"),
      (4, "id3", 40, "e"),
      (5, "id2", null.asInstanceOf[Int], null)))
  }

  test("leftOuterJoin using") {
    val leftOuter: TypedDataFrame[(Int, String, Int, String)] = fooTF.leftOuterJoin(s1).using('a)
    checkAnswer(leftOuter, Set(
      (1, "id1", 10, "c"),
      (4, "id3", 40, "e"),
      (5, "id2", null.asInstanceOf[Int], null)))
  }

  test("rightOuterJoin using") {
    val rightOuter: TypedDataFrame[(Int, String, Int, String)] = fooTF.rightOuterJoin(s1).using('a)
    checkAnswer(rightOuter, Set(
      (1, "id1", 10, "c"),
      (2, null, 20, "d"),
      (4, "id3", 40, "e")))
  }

  test("leftsemiJoin using") {
    val leftsemi: TypedDataFrame[Foo] = fooTF.leftsemiJoin(s1).using('a)
    checkAnswer(leftsemi, Set(
      Foo(1, "id1"),
      Foo(4, "id3")))
  }

  test("innerJoin on") {
    val inner: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.innerJoin(s1).on('a).and('a)
    checkAnswer(inner, Set(
      (1, "id1", 1,  10, "c"),
      (4, "id3", 4,  40, "e")))

    val inner2: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.innerJoin(s1).on('a).and('b)
    checkAnswer(inner2, Set.empty[(Int, String, Int, Int, String)])

    val inner3: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.innerJoin(s1).on('a, 'b).and('a, 'c)
    checkAnswer(inner3, Set.empty[(Int, String, Int, Int, String)])

    illTyped("fooTF.innerJoin(s1).on('b).and('a)")
    illTyped("fooTF.innerJoin(s1).on('c).and('a)")
    illTyped("fooTF.innerJoin(s1).on('a, 'b).and('a)")
    illTyped("fooTF.innerJoin(s1).on('a, 'b).and('c)")
    illTyped("fooTF.innerJoin(s1).on('a, 'b).and('a, 'b)")
    illTyped("fooTF.innerJoin(s1).on('a, 'b).and('a, 'b, 'c)")
  }

  test("outerJoin on") {
    val outer: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.outerJoin(s1).on('a).and('a)
    checkAnswer(outer, Set(
      (1, "id1", 1,  10, "c"),
      (null.asInstanceOf[Int], null, 2,  20, "d"),
      (4, "id3", 4,  40, "e"),
      (5, "id2", null.asInstanceOf[Int],  null.asInstanceOf[Int], null)))
  }

  test("leftOuterJoin on") {
    val leftOuter: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.leftOuterJoin(s1).on('a).and('a)
    checkAnswer(leftOuter, Set(
      (1, "id1", 1,  10, "c"),
      (4, "id3", 4,  40, "e"),
      (5, "id2", null.asInstanceOf[Int],  null.asInstanceOf[Int], null)))
  }

  test("rightOuterJoin on") {
    val rightOuter: TypedDataFrame[(Int, String, Int, Int, String)] = fooTF.rightOuterJoin(s1).on('a).and('a)
    checkAnswer(rightOuter, Set(
      (1, "id1", 1,  10, "c"),
      (null.asInstanceOf[Int], null, 2,  20, "d"),
      (4, "id3", 4,  40, "e")))
  }

  test("leftsemiJoin on") {
    val leftsemi: TypedDataFrame[Foo] = fooTF.leftsemiJoin(s1).on('a).and('a)
    checkAnswer(leftsemi, Set(
      Foo(1, "id1"),
      Foo(4, "id3")))
  }
}
