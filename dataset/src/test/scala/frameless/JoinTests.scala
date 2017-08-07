package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class JoinTests extends TypedDatasetSuite {
  test("ab.joinRight(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinRight(rightDs)(leftDs.col('a) === rightDs.col('a))
        .collect().run().toVector.sorted

      val leftKeys = left.map(_.a).toSet
      val joined = {
        for {
          ab <- left
          ac <- right if ac.a == ab.a
        } yield (Some(ab), ac)
      }.toVector ++ {
        for {
          ac <- right if !leftKeys.contains(ac.a)
        } yield (None, ac)
      }.toVector

      (joined.sorted ?= joinedDs) && (joinedDs.map(_._2).toSet ?= right.toSet)
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("ab.joinInner(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinInner(rightDs)(leftDs.col('a) === rightDs.col('a))
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

  test("ab.joinLeft(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinLeft(rightDs)(leftDs.col('a) === rightDs.col('a))
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

  test("ab.joinFull(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinFull(rightDs)(leftDs.col('a) === rightDs.col('a))
        .collect().run().toVector.sorted

      val rightKeys = right.map(_.a).toSet
      val leftKeys  = left.map(_.a).toSet
      val joined = {
        for {
          ab <- left
          ac <- right if ac.a == ab.a
        } yield (Some(ab), Some(ac))
      }.toVector ++ {
        for {
          ab <- left if !rightKeys.contains(ab.a)
        } yield (Some(ab), None)
      }.toVector ++ {
        for {
          ac <- right if !leftKeys.contains(ac.a)
        } yield (None, Some(ac))
      }.toVector

      (joined.sorted ?= joinedDs)
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("ab.joinCross(ac)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = leftDs
        .joinCross(rightDs)
        .collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left
          ac <- right
        } yield (ab, ac)
      }.toVector

      (joined.sorted ?= joinedDs)
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("ab.joinLeftSemi(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val rightKeys = right.map(_.a).toSet
      val joinedDs = leftDs
        .joinLeftSemi(rightDs)(leftDs.col('a) === rightDs.col('a))
        .collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left if rightKeys.contains(ab.a)
        } yield ab
      }.toVector

      joined.sorted ?= joinedDs
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("ab.joinLeftAnti(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val rightKeys = right.map(_.a).toSet
      val joinedDs = leftDs
        .joinLeftAnti(rightDs)(leftDs.col('a) === rightDs.col('a))
        .collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left if !rightKeys.contains(ab.a)
        } yield ab
      }.toVector

      joined.sorted ?= joinedDs
    }

    check(forAll(prop[Int, Long, String] _))
  }

  test("self inner join") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering
    ](data: List[X2[A, B]]): Prop = {
      val ds = TypedDataset.create(data)

      val count = ds.dataset.join(ds.dataset, ds.dataset.col("a") === ds.dataset.col("a")).count()

      val countDs = ds.joinInner(ds)(ds.col('a) === ds.col('a))
        .count().run()

      count ?= countDs
    }

    check(prop[Int, Int] _)
  }
}
