package frameless

import scala.collection.immutable.{Set, TreeSet}
import org.scalatest.matchers.should.Matchers

object EncoderTests {
  case class Foo(s: Seq[(Int, Int)])
  case class Bar(s: Set[(Int, Int)])
  case class InstantRow(i: java.time.Instant)
  case class DurationRow(d: java.time.Duration)
  case class PeriodRow(p: java.time.Period)

  case class VectorOfObject(a: Vector[X1[Int]])

  case class TreeSetOfObjects(a: TreeSet[X1[Int]])
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

  test("It should encode a Vector of Objects") {
    evalCodeGens {
      implicit val e = implicitly[TypedEncoder[VectorOfObject]]
      implicit val te = TypedExpressionEncoder[VectorOfObject]
      implicit val xe = implicitly[TypedEncoder[X1[VectorOfObject]]]
      implicit val xte = TypedExpressionEncoder[X1[VectorOfObject]]
      val v = (1 to 20).map(X1(_)).toVector
      val ds = {
        sqlContext.createDataset(Seq(X1[VectorOfObject](VectorOfObject(v))))
      }
      ds.head.a.a shouldBe v
    }
  }

  test("It should encode a TreeSet of Objects") {
    evalCodeGens {
      implicit val e = implicitly[TypedEncoder[TreeSetOfObjects]]
      implicit val te = TypedExpressionEncoder[TreeSetOfObjects]
      implicit val xe = implicitly[TypedEncoder[X1[TreeSetOfObjects]]]
      implicit val xte = TypedExpressionEncoder[X1[TreeSetOfObjects]]
      val v = (1 to 20).map(X1(_)).to[TreeSet]
      val ds = {
        sqlContext.createDataset(Seq(X1[TreeSetOfObjects](TreeSetOfObjects(v))))
      }
      ds.head.a.a shouldBe v
    }
  }
}
