package frameless

import org.apache.spark.sql.types.{StructField, StructType}
import org.scalacheck.Prop
import org.scalacheck.Prop._

class JoinTests extends TypedDatasetSuite {
  test("ab.joinCross(ac)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[(X2[A,B], X2[A,C])])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left
          ac <- right
        } yield (ab, ac)
      }.toVector

      val equalSchemas = joinedDs.schema ?= StructType(Seq(
        StructField("_1", leftDs.schema, nullable = false),
        StructField("_2", rightDs.schema, nullable = false)))

      (joined.sorted ?= joinedData) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .joinCross(rightDs)) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).cross) _))
  }

  test("ab.joinFull(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[(Option[X2[A,B]], Option[X2[A,C]])])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

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

      val equalSchemas = joinedDs.schema ?= StructType(Seq(
        StructField("_1", leftDs.schema, nullable = true),
        StructField("_2", rightDs.schema, nullable = true)))

      (joined.sorted ?= joinedData) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .joinFull(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).full(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).full(_('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).full(_.col('a) === _.col('a))) _))
  }

  test("ab.joinInner(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[(X2[A,B], X2[A,C])])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left
          ac <- right if ac.a == ab.a
        } yield (ab, ac)
      }.toVector

      val equalSchemas = joinedDs.schema ?= StructType(Seq(
        StructField("_1", leftDs.schema, nullable = false),
        StructField("_2", rightDs.schema, nullable = false)))

      (joined.sorted ?= joinedData) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .joinInner(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).inner(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).inner(_.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).inner(_.col('a) === _.col('a))) _))
  }

  test("ab.joinLeft(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[(X2[A,B], Option[X2[A,C]])])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = f(leftDs,rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

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

      val equalSchemas = joinedDs.schema ?= StructType(Seq(
        StructField("_1", leftDs.schema, nullable = false),
        StructField("_2", rightDs.schema, nullable = true)))

      (joined.sorted ?= joinedData) && (joinedData.map(_._1).toSet ?= left.toSet) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .joinLeft(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).left(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).left(_.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).left(_.col('a) === _.col('a))) _))
  }

  test("ab.joinLeftAnti(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[X2[A,B]])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val rightKeys = right.map(_.a).toSet
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left if !rightKeys.contains(ab.a)
        } yield ab
      }.toVector

      val equalSchemas = joinedDs.schema ?= leftDs.schema

      (joined.sorted ?= joinedData) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs,rightDs) => leftDs
      .joinLeftAnti(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftAnti(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftAnti(_.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftAnti(_.col('a) === _.col('a))) _))
  }

  test("ab.joinLeftSemi(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[X2[A,B]])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val rightKeys = right.map(_.a).toSet
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

      val joined = {
        for {
          ab <- left if rightKeys.contains(ab.a)
        } yield ab
      }.toVector

      val equalSchemas = joinedDs.schema ?= leftDs.schema

      (joined.sorted ?= joinedData) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .joinLeftSemi(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftSemi(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftSemi(_.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) => leftDs
      .join(rightDs).leftSemi(_.col('a) === _.col('a))) _))
  }

  test("ab.joinRight(ac)(ab.a == ac.a)") {
    def prop[
      A : TypedEncoder : Ordering,
      B : TypedEncoder : Ordering,
      C : TypedEncoder : Ordering
    ](f: (TypedDataset[X2[A,B]], TypedDataset[X2[A,C]]) => TypedDataset[(Option[X2[A,B]], X2[A,C])])(left: List[X2[A, B]], right: List[X2[A, C]]): Prop = {
      val leftDs = TypedDataset.create(left)
      val rightDs = TypedDataset.create(right)
      val joinedDs = f(leftDs, rightDs)

      val joinedData = joinedDs.collect().run().toVector.sorted

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

      val equalSchemas = joinedDs.schema ?= StructType(Seq(
        StructField("_1", leftDs.schema, nullable = true),
        StructField("_2", rightDs.schema, nullable = false)))

      (joined.sorted ?= joinedData) && (joinedData.map(_._2).toSet ?= right.toSet) && equalSchemas
    }

    check(forAll(prop[Int, Long, String]((leftDs, rightDs) =>
      leftDs.joinRight(rightDs)(leftDs.col('a) === rightDs.col('a))) _))

    import frameless.syntax.ChainedJoinSyntax
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) =>
      leftDs.join(rightDs).right(leftDs.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) =>
      leftDs.join(rightDs).right(_.col('a) === rightDs.col('a))) _))
    check(forAll(prop[Int, Long, String]((leftDs, rightDs) =>
      leftDs.join(rightDs).right(_.col('a) === _.col('a))) _))
  }

  test("chained") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder : Ordering,
      C: TypedEncoder : Ordering,
      D: TypedEncoder : Ordering
    ](left: Seq[X2[A, B]], mid: Seq[X2[A, C]], right: Seq[X2[C, D]] ): Unit = {
      val leftDs = TypedDataset.create(left)
      val midDs = TypedDataset.create(mid)
      val rightDs = TypedDataset.create(right)
      /* orig
      val joinedDs1 = leftDs
        .joinRight(midDs)(leftDs.col('a) === midDs.col('a))
      val joinedDs = joinedDs1
        .joinRight(rightDs)(joinedDs1.col('_2).field('b) === rightDs.col('a))
      */
      import frameless.syntax.ChainedJoinSyntax

      // join right
      val joinedDs = leftDs
        .join(midDs).right(leftDs.col('a) === midDs.col('a))
        .join(rightDs).right(t => t.col('_2).field('b) === rightDs.col('a))

      //joinedDs.show().run()

      val joinedData = joinedDs.collect().run().toVector.sorted
      assert(joinedData == Seq(
        (Some((Some(X2(1,1L)), X2(1,5L))), X2(5L, "5s")),
        (Some((Some(X2(2,2L)), X2(2,6L))), X2(6L, "6s")),
        (Some((Some(X2(3,3L)), X2(3,7L))), X2(7L, "7s"))
      ))

      ()
    }

    prop[Int, Long, Long, String](Seq(X2(1,1L), X2(2,2L), X2(3,3L)), Seq(X2(1,5L), X2(2,6L), X2(3,7L)),
      Seq(X2(5L, "5s"), X2(6L, "6s"), X2(7L, "7s")))
  }
}
