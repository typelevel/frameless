package frameless

import scala.collection.immutable.{ListSet, Set, TreeSet}
import org.scalatest.matchers.should.Matchers

object EncoderTests {
  case class Foo(s: Seq[(Int, Int)])
  case class Bar(s: Set[(Int, Int)])
  case class InstantRow(i: java.time.Instant)
  case class DurationRow(d: java.time.Duration)
  case class PeriodRow(p: java.time.Period)

  case class ContainerOf[CC[X] <: Iterable[X]](a: CC[X1[Int]])
}

class EncoderTests extends TypedDatasetSuite with Matchers {
  import EncoderTests._

  test("It should encode deeply nested collections") {
    implicitly[TypedEncoder[Seq[Foo]]]
    implicitly[TypedEncoder[Seq[Bar]]]
    implicitly[TypedEncoder[Set[Foo]]]
  }

  test("It should encode java.time.Instant") {
    implicitly[TypedEncoder[InstantRow]]
  }

  test("It should encode java.time.Duration") {
    implicitly[TypedEncoder[DurationRow]]
  }

  test("It should encode java.time.Period") {
    implicitly[TypedEncoder[PeriodRow]]
  }

  def performCollection[C[X] <: Iterable[X]](toType: Seq[X1[Int]] => C[X1[Int]])(implicit ce: TypedEncoder[C[X1[Int]]]): (Unit,Unit) = evalCodeGens {

    implicit val cte = TypedExpressionEncoder[C[X1[Int]]]
    implicit val e = implicitly[TypedEncoder[ContainerOf[C]]]
    implicit val te = TypedExpressionEncoder[ContainerOf[C]]
    implicit val xe = implicitly[TypedEncoder[X1[ContainerOf[C]]]]
    implicit val xte = TypedExpressionEncoder[X1[ContainerOf[C]]]
    val v = toType((1 to 20).map(X1(_)))
    val ds = {
      sqlContext.createDataset(Seq(X1[ContainerOf[C]](ContainerOf[C](v))))
    }
    ds.head.a.a shouldBe v
    ()
  }

  test("It should serde a Seq of Objects") {
    performCollection[Seq](_)
  }

  test("It should serde a Set of Objects") {
    performCollection[Set](_)
  }

  test("It should serde a Vector of Objects") {
    performCollection[Vector](_.toVector)
  }

  test("It should serde a TreeSet of Objects") {
    // only needed for 2.12
    implicit val ordering = new Ordering[X1[Int]] {
      val intordering = implicitly[Ordering[Int]]
      override def compare(x: X1[Int], y: X1[Int]): Int = intordering.compare(x.a, y.a)
    }

    performCollection[TreeSet](TreeSet.newBuilder.++=(_).result())
  }

  test("It should serde a List of Objects") {
    performCollection[List](_.toList)
  }

  test("It should serde a ListSet of Objects") {
    performCollection[ListSet](ListSet.newBuilder.++=(_).result())
  }
}
