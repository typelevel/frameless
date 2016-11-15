package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class JoinTests extends TypedDatasetSuite {
  test("ab.joinLeft(ac, ab.a, ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinLeft(rightDs, leftDs.col('a), rightDs.col('a))
        .collect().run().toVector.sorted

      val rightKeys = right.map(_.a).toSet
      val joined = {
        for {
          ab <- left
          ac <- right if ac.a == ab.a
        } yield (ab, Some(ac))
      }.toVector ++ {
        for {
          ab <- left if !rightKeys.contains(ab.a)
        } yield (ab, None)
      }.toVector

      (joined.sorted ?= joinedDs) && (joinedDs.map(_._1).toSet ?= left.toSet)
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("ab.join(ac, ab.a, ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .join(rightDs, leftDs.col('a), rightDs.col('a))
        .collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left
          ac <- right if ac.a == ab.a
        } yield (ab, ac)
      }.toVector

      joined.sorted ?= joinedDs
    }

    check(forAll(prop[Int, Long, String] _))
  }
}
