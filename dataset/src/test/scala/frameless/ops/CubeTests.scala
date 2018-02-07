package frameless
package ops

import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class CubeTests extends TypedDatasetSuite {

  test("cube('a).agg(count())") {
    def prop[A: TypedEncoder : Ordering, Out: TypedEncoder : Numeric]
    (data: List[X1[A]])(implicit summable: CatalystSummable[A, Out]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val received = dataset.cube(A).agg(count()).collect().run().toVector.sortBy(_._2)
      val expected = dataset.dataset.cube("a").count().collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[Long](1))).sortBy(_._2)

      received ?= expected
    }

    check(forAll(prop[Int, Long] _))
  }

  test("cube('a, 'b).agg(count())") {
    def prop[A: TypedEncoder : Ordering, B: TypedEncoder, Out: TypedEncoder : Numeric]
    (data: List[X2[A, B]])(implicit summable: CatalystSummable[B, Out]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val received = dataset.cube(A, B).agg(count()).collect().run().toVector.sortBy(_._3)
      val expected = dataset.dataset.cube("a", "b").count().collect().toVector
        .map(row => (Option(row.getAs[A](0)), Option(row.getAs[B](1)), row.getAs[Long](2))).sortBy(_._3)

      received ?= expected
    }

    check(forAll(prop[Int, Long, Long] _))
  }

  test("cube('a).agg(sum('b)") {
    def prop[A: TypedEncoder : Ordering, B: TypedEncoder, Out: TypedEncoder : Numeric]
    (data: List[X2[A, B]])(implicit summable: CatalystSummable[B, Out]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val received = dataset.cube(A).agg(sum(B)).collect().run().toVector.sortBy(_._2)
      val expected = dataset.dataset.cube("a").sum("b").collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[Out](1))).sortBy(_._2)

      received ?= expected
    }

    check(forAll(prop[Int, Long, Long] _))
  }

  test("cube('a).mapGroups('a, sum('b))") {
    def prop[A: TypedEncoder : Ordering, B: TypedEncoder : Numeric]
    (data: List[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val received = dataset.cube(A)
        .deserialized.mapGroups { case (a, xs) => (a, xs.map(_.b).sum) }
        .collect().run().toVector.sortBy(_._1)
      val expected = data.groupBy(_.a).mapValues(_.map(_.b).sum).toVector.sortBy(_._1)

      received ?= expected
    }

    check(forAll(prop[Int, Long] _))
  }

  test("cube('a).agg(sum('b), sum('c)) to cube('a).agg(sum('a), sum('b), sum('a), sum('b), sum('a))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder,
    C: TypedEncoder,
    OutB: TypedEncoder : Numeric,
    OutC: TypedEncoder : Numeric
    ](data: List[X3[A, B, C]])(
      implicit
      summableB: CatalystSummable[B, OutB],
      summableC: CatalystSummable[C, OutC]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val framelessSumBC = dataset
        .cube(A)
        .agg(sum(B), sum(C))
        .collect().run().toVector.sortBy(_._1)

      val sparkSumBC = dataset.dataset.cube("a").sum("b", "c").collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[OutB](1), row.getAs[OutC](2)))
        .sortBy(_._1)

      val framelessSumBCB = dataset
        .cube(A)
        .agg(sum(B), sum(C), sum(B))
        .collect().run().toVector.sortBy(_._1)

      val sparkSumBCB = dataset.dataset.cube("a").sum("b", "c", "b").collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[OutB](1), row.getAs[OutC](2), row.getAs[OutB](3)))
        .sortBy(_._1)

      val framelessSumBCBC = dataset
        .cube(A)
        .agg(sum(B), sum(C), sum(B), sum(C))
        .collect().run().toVector.sortBy(_._1)

      val sparkSumBCBC = dataset.dataset.cube("a").sum("b", "c", "b", "c").collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[OutB](1), row.getAs[OutC](2), row.getAs[OutB](3), row.getAs[OutC](4)))
        .sortBy(_._1)

      val framelessSumBCBCB = dataset
        .cube(A)
        .agg(sum(B), sum(C), sum(B), sum(C), sum(B))
        .collect().run().toVector.sortBy(_._1)

      val sparkSumBCBCB = dataset.dataset.cube("a").sum("b", "c", "b", "c", "b").collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[OutB](1), row.getAs[OutC](2), row.getAs[OutB](3), row.getAs[OutC](4), row.getAs[OutB](5)))
        .sortBy(_._1)

      (framelessSumBC ?= sparkSumBC)
        .&&(framelessSumBCB ?= sparkSumBCB)
        .&&(framelessSumBCBC ?= sparkSumBCBC)
        .&&(framelessSumBCBCB ?= sparkSumBCBCB)
    }

    check(forAll(prop[String, Long, BigDecimal, Long, BigDecimal] _))
  }

  test("cube('a, 'b).agg(sum('c), sum('d))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering,
    C: TypedEncoder,
    D: TypedEncoder,
    OutC: TypedEncoder : Numeric,
    OutD: TypedEncoder : Numeric
    ](data: List[X4[A, B, C, D]])(
      implicit
      summableC: CatalystSummable[C, OutC],
      summableD: CatalystSummable[D, OutD]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)
      val D = dataset.col[D]('d)

      val framelessSumByAB = dataset
        .cube(A, B)
        .agg(sum(C), sum(D))
        .collect().run().toVector.sortBy(x => (x._1, x._2))

      val sparkSumByAB = dataset.dataset
        .cube("a", "b").sum("c", "d").collect().toVector
        .map(row => (Option(row.getAs[A](0)), Option(row.getAs[B](1)), row.getAs[OutC](2), row.getAs[OutD](3)))
        .sortBy(x => (x._1, x._2))

      framelessSumByAB ?= sparkSumByAB
    }

    check(forAll(prop[Byte, Int, Long, BigDecimal, Long, BigDecimal] _))
  }

  test("cube('a, 'b).mapGroups('a, 'b, sum('c))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering,
    C: TypedEncoder : Numeric
    ](data: List[X3[A, B, C]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val framelessSumByAB = dataset
        .cube(A, B)
        .deserialized.mapGroups { case ((a, b), xs) => (a, b, xs.map(_.c).sum) }
        .collect().run().toVector.sortBy(x => (x._1, x._2))

      val sumByAB = data.groupBy(x => (x.a, x.b))
        .mapValues { xs => xs.map(_.c).sum }
        .toVector.map { case ((a, b), c) => (a, b, c) }.sortBy(x => (x._1, x._2))

      framelessSumByAB ?= sumByAB
    }

    check(forAll(prop[Byte, Int, Long] _))
  }

  test("cube('a).mapGroups(('a, toVector(('a, 'b))") {
    def prop[
    A: TypedEncoder,
    B: TypedEncoder
    ](data: Vector[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val datasetGrouped = dataset
        .cube(A)
        .deserialized.mapGroups((a, xs) => (a, xs.toVector))
        .collect().run().toMap

      val dataGrouped = data.groupBy(_.a)

      datasetGrouped ?= dataGrouped
    }

    check(forAll(prop[Short, Option[Short]] _))
    check(forAll(prop[Option[Short], Short] _))
    check(forAll(prop[X1[Option[Short]], Short] _))
  }

  test("cube('a).flatMapGroups(('a, toVector(('a, 'b))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering
    ](data: Vector[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val datasetGrouped = dataset
        .cube(A)
        .deserialized.flatMapGroups((a, xs) => xs.map(x => (a, x)))
        .collect().run()
        .sorted

      val dataGrouped = data
        .groupBy(_.a).toSeq
        .flatMap { case (a, xs) => xs.map(x => (a, x)) }
        .sorted

      datasetGrouped ?= dataGrouped
    }

    check(forAll(prop[Short, Option[Short]] _))
    check(forAll(prop[Option[Short], Short] _))
    check(forAll(prop[X1[Option[Short]], Short] _))
  }

  test("cube('a, 'b).flatMapGroups((('a,'b) toVector((('a,'b), 'c))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering,
    C: TypedEncoder : Ordering
    ](data: Vector[X3[A, B, C]]): Prop = {
      val dataset = TypedDataset.create(data)
      val cA = dataset.col[A]('a)
      val cB = dataset.col[B]('b)

      val datasetGrouped = dataset
        .cube(cA, cB)
        .deserialized.flatMapGroups((a, xs) => xs.map(x => (a, x)))
        .collect().run()
        .sorted

      val dataGrouped = data
        .groupBy(t => (t.a, t.b)).toSeq
        .flatMap { case (a, xs) => xs.map(x => (a, x)) }
        .sorted

      datasetGrouped ?= dataGrouped
    }

    check(forAll(prop[Short, Option[Short], Long] _))
    check(forAll(prop[Option[Short], Short, Int] _))
    check(forAll(prop[X1[Option[Short]], Short, Byte] _))
  }

  test("cubeMany('a).agg(sum('b))") {
    def prop[A: TypedEncoder : Ordering, Out: TypedEncoder : Numeric]
    (data: List[X1[A]])(implicit summable: CatalystSummable[A, Out]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val received = dataset.cubeMany(A).agg(count()).collect().run().toVector.sortBy(_._2)
      val expected = dataset.dataset.cube("a").count().collect().toVector
        .map(row => (Option(row.getAs[A](0)), row.getAs[Long](1))).sortBy(_._2)

      received ?= expected
    }

    check(forAll(prop[Int, Long] _))
  }
}