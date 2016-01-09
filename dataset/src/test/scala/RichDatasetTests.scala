import org.apache.spark.sql.{SpecWithContext, Dataset}
import shapeless.test.illTyped
import frameless._

case class Foo(a: Int, b: String)
case class Bar(i: Int, s: String)
case class XXX(a: Int, b: Double)

class RichDatasetTests extends SpecWithContext {
  import testImplicits._
  
  val fooTuples: Seq[(Int, String)] = Seq((1, "id1"), (4, "id3"), (5, "id2"))
  def fooDs: Dataset[Foo] = fooTuples.toDF.as[Foo]
  
  test("typedAs") {
    fooDs.typedAs[Foo]()
    fooDs.typedAs[Bar]()
    illTyped("fooDs.typedAs[XXX]()")
  }
}
