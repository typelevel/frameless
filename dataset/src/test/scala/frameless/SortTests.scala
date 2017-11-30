package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.apache.spark.sql.{functions => sfunc}
import frameless.functions.aggregate._

object SortTests {
  case class Wack(w: Int)
  case class Foo(a: String, b: Option[Int], c: Array[String], d: Map[String, Int], wack: Wack)
}

//TODO:
class SortTests extends TypedDatasetSuite {
  import SortTests._

  //I think this is the ordering that Spark would use
  implicit def arrayOrdering[T: Ordering]: Ordering[Array[T]] = {
    Ordering.by((a: Array[T]) => a.map(_.toString).mkString)
  }

  def optionNullFirstOrdering[T : Ordering] = new Ordering[Option[T]] {
    override def compare(x: Option[T], y: Option[T]) = (x, y) match {
      case (None, None)       => 0
      case (_, None)          => -1
      case (None, _)          => 1
      case (Some(x), Some(y)) => implicitly[Ordering[T]].compare(x, y)
    }
  }

//  test("sort asc") {
//    def prop[A : TypedEncoder : frameless.CatalystRowOrdered : Ordering](data: Vector[X1[A]]) = {
//      val dataset = TypedDataset.create(data)
//
//      val sorted: Vector[X1[A]] = dataset
//        .sort(dataset('a).asc)
//        .collect()
//        .run()
//        .toVector
//
//      val sortedVector = data.sortBy(_.a)
//
//
//      assert(sorted.map(_.a) === sortedVector.map(_.a))
////      sorted === sortedVector
////
////      withClue(sortedVector.map(_.toString).mkString) {
////        sorted.zip(sortedVector).foldLeft(passed) { case (a, (l, r)) =>
////          a && (l.a === r.a)
////        }
////      }
////      sorted ?= sortedVector
//    }
//
//    check(forAll(prop[Int] _))
//    check(forAll(prop[String] _))
//    check(forAll(prop[Array[Int]] _))
//  }

  pending("asdf") {
    val seq = Seq(
      Foo("a", Some(2), Array("a", "b"), Map("world" -> 2), Wack(1)),
      Foo("b", Some(1), Array("b", "a"), Map("world" -> 2), Wack(2))
    )

    val ds = TypedDataset.create(seq)

    assert(ds.sort(ds('a).asc).collect().run() === seq.sortBy(_.a))
    assert(ds.sort(ds('a).desc).collect().run() === seq.sortBy(_.a)(Ordering[String].reverse))

    assert(ds.sort(ds('b).asc).collect().run() === seq.sortBy(_.b))
    assert(ds.sort(ds('b).desc).collect().run() === seq.sortBy(_.b)(Ordering[Option[Int]].reverse))

    assert(ds.sort(ds('b).asc_nulls_first).collect().run() === seq.sortBy(_.b)(optionNullFirstOrdering[Int]))


//    ds.sort(ds('b).isNone.asc).show().run()

    /* This won't even compile instead of blowing at runtime!
    ds.sort(ds('d).desc).show().run()
    */

    /* TODO: order by struct
    ds.sort(ds('wack).desc).show().run()
    */

  }

}
