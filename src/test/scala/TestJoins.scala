import org.apache.spark.sql.SpecWithContext
import shapeless.test.illTyped
import typedframe._

class TestJoins extends SpecWithContext {
  import testImplicits._
  
  def fooTF: TypedFrame[Foo] = Seq((1, "id1"), (4, "id3"), (5, "id2")).toDF.toTF

  test("cartesianJoin") {
    case class Bar(i: Double, j: String)
    val bar: TypedFrame[Bar] = Seq((1.1, "s"), (2.2, "t")).toDF.toTF
    val p = fooTF.cartesianJoin(bar): TypedFrame[(Int, String, Double, String)]
    
    checkAnswer(p, Set(
      (1, "id1", 1.1, "s"), (4, "id3", 1.1, "s"), (5, "id2", 1.1, "s"),
      (1, "id1", 2.2, "t"), (4, "id3", 2.2, "t"), (5, "id2", 2.2, "t")))
  }
  
  case class Schema1(a: Int, b: Int, c: String)
  def s1: TypedFrame[Schema1] = Seq((1, 10, "c"), (2, 20, "d"), (4, 40, "e")).toDF.toTF
  
  def nullInstance[T] = null.asInstanceOf[T]
  
  test("joinUsing") {
    // val inner: TypedFrame[(Int, String, Int, String)] = fooTF.innerJoin(s1).using('a)
    // checkAnswer(inner, Set((1, "id1", 10, "c"), (4, "id3", 40, "e")))
    
    val outer: TypedFrame[(Int, String, Int, String)] = fooTF.outerJoin(s1).using('a)
    
    // println("outer.show()")
    // outer.show()
    // println("outer.collect()")
    // println(outer.collect())
    
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
    // val s2: TypedFrame[Schema2] = Seq((1, "10", "c"), (2, "20", "d"), (4, "40", "e")).toDF.toTF
    
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
  //   val s2: TypedFrame[Schema2] = Seq(("10", 1, 10, "c"), ("20", 2, 20, "d"), ("40", 4, 40, "e")).toDF.toTF
    
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
}
