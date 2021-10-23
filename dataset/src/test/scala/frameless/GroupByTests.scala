package frameless

import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class GroupByTests extends TypedDatasetSuite {
  test("groupByMany('a).agg(sum('b))") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder,
      Out: TypedEncoder : Numeric
    ](data: List[X2[A, B]])(
      implicit
      summable: CatalystSummable[B, Out],
      widen: B => Out
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val datasetSumByA = dataset.groupByMany(A).agg(sum(B)).collect().run.toVector.sortBy(_._1)
      val sumByA = data.groupBy(_.a).mapValues(_.map(_.b).map(widen).sum).toVector.sortBy(_._1)

      datasetSumByA ?= sumByA
    }

    check(forAll(prop[Int, Long, Long] _))
  }

  test("agg(sum('a))") {
    def prop[A: TypedEncoder : Numeric](data: List[X1[A]])(
      implicit
      summable: CatalystSummable[A, A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val datasetSum = dataset.agg(sum(A)).collect().run().toVector
      val listSum = data.map(_.a).sum

      datasetSum ?= Vector(listSum)
    }

    check(forAll(prop[Long] _))
  }

  test("agg(sum('a), sum('b))") {
    def prop[
      A: TypedEncoder : Numeric,
      B: TypedEncoder : Numeric
    ](data: List[X2[A, B]])(
      implicit
      as: CatalystSummable[A, A],
      bs: CatalystSummable[B, B]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val datasetSum = dataset.agg(sum(A), sum(B)).collect().run().toVector
      val listSumA = data.map(_.a).sum
      val listSumB = data.map(_.b).sum

      datasetSum ?= Vector((listSumA, listSumB))
    }

    check(forAll(prop[Long, Long] _))
  }

  test("agg(sum('a), sum('b), sum('c))") {
    def prop[
    A: TypedEncoder : Numeric,
    B: TypedEncoder : Numeric,
    C: TypedEncoder : Numeric
    ](data: List[X3[A, B, C]])(
      implicit
      as: CatalystSummable[A, A],
      bs: CatalystSummable[B, B],
      cs: CatalystSummable[C, C]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val datasetSum = dataset.agg(sum(A), sum(B), sum(C)).collect().run().toVector
      val listSumA = data.map(_.a).sum
      val listSumB = data.map(_.b).sum
      val listSumC = data.map(_.c).sum

      datasetSum ?= Vector((listSumA, listSumB, listSumC))
    }

    check(forAll(prop[Long, Long, Long] _))
  }

  test("agg(sum('a), sum('b), min('c), max('d))") {
    def prop[
    A: TypedEncoder : Numeric,
    B: TypedEncoder : Numeric,
    C: TypedEncoder : Numeric,
    D: TypedEncoder : Numeric
    ](data: List[X4[A, B, C, D]])(
      implicit
      as: CatalystSummable[A, A],
      bs: CatalystSummable[B, B],
      co: CatalystOrdered[C],
      fo: CatalystOrdered[D]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)
      val D = dataset.col[D]('d)

      val datasetSum = dataset.agg(sum(A), sum(B), min(C), max(D)).collect().run().toVector
      val listSumA = data.map(_.a).sum
      val listSumB = data.map(_.b).sum
      val listMinC = if(data.isEmpty) implicitly[Numeric[C]].fromInt(0) else data.map(_.c).min
      val listMaxD = if(data.isEmpty) implicitly[Numeric[D]].fromInt(0) else data.map(_.d).max

      datasetSum ?= Vector(if (data.isEmpty) null else (listSumA, listSumB, listMinC, listMaxD))
    }

    check(forAll(prop[Long, Long, Long, Int] _))
    check(forAll(prop[Long, Long, Short, Short] _))
    check(forAll(prop[Long, Long, Double, BigDecimal] _))
  }

  test("groupBy('a).agg(sum('b))") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder,
      Out: TypedEncoder : Numeric
    ](data: List[X2[A, B]])(
      implicit
      summable: CatalystSummable[B, Out],
      widen: B => Out
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val datasetSumByA = dataset.groupBy(A).agg(sum(B)).collect().run.toVector.sortBy(_._1)
      val sumByA = data.groupBy(_.a).mapValues(_.map(_.b).map(widen).sum).toVector.sortBy(_._1)

      datasetSumByA ?= sumByA
    }

    check(forAll(prop[Int, Long, Long] _))
  }

  test("groupBy('a).mapGroups('a, sum('b))") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder : Numeric
    ](data: List[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val datasetSumByA = dataset.groupBy(A)
        .deserialized.mapGroups { case (a, xs) => (a, xs.map(_.b).sum) }
        .collect().run().toVector.sortBy(_._1)
      val sumByA = data.groupBy(_.a).mapValues(_.map(_.b).sum).toVector.sortBy(_._1)

      datasetSumByA ?= sumByA
    }

    check(forAll(prop[Int, Long] _))
  }

  test("groupBy('a).agg(sum('b), sum('c)) to groupBy('a).agg(sum('a), sum('b), sum('a), sum('b), sum('a))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder,
    C: TypedEncoder,
    OutB: TypedEncoder : Numeric,
    OutC: TypedEncoder : Numeric
    ](data: List[X3[A, B, C]])(
      implicit
      summableB: CatalystSummable[B, OutB],
      summableC: CatalystSummable[C, OutC],
      widenb: B => OutB,
      widenc: C => OutC
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val framelessSumBC = dataset
        .groupBy(A)
        .agg(sum(B), sum(C))
        .collect().run.toVector.sortBy(_._1)

      val scalaSumBC = data.groupBy(_.a).mapValues { xs =>
        (xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum)
      }.toVector.map {
        case (a, (b, c)) => (a, b, c)
      }.sortBy(_._1)

      val framelessSumBCB = dataset
        .groupBy(A)
        .agg(sum(B), sum(C), sum(B))
        .collect().run.toVector.sortBy(_._1)

      val scalaSumBCB = data.groupBy(_.a).mapValues { xs =>
        (xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum, xs.map(_.b).map(widenb).sum)
      }.toVector.map {
        case (a, (b1, c, b2)) => (a, b1, c, b2)
      }.sortBy(_._1)

      val framelessSumBCBC = dataset
        .groupBy(A)
        .agg(sum(B), sum(C), sum(B), sum(C))
        .collect().run.toVector.sortBy(_._1)

      val scalaSumBCBC = data.groupBy(_.a).mapValues { xs =>
        (xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum, xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum)
      }.toVector.map {
        case (a, (b1, c1, b2, c2)) => (a, b1, c1, b2, c2)
      }.sortBy(_._1)

      val framelessSumBCBCB = dataset
        .groupBy(A)
        .agg(sum(B), sum(C), sum(B), sum(C), sum(B))
        .collect().run.toVector.sortBy(_._1)

      val scalaSumBCBCB = data.groupBy(_.a).mapValues { xs =>
        (xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum, xs.map(_.b).map(widenb).sum, xs.map(_.c).map(widenc).sum, xs.map(_.b).map(widenb).sum)
      }.toVector.map {
        case (a, (b1, c1, b2, c2, b3)) => (a, b1, c1, b2, c2, b3)
      }.sortBy(_._1)

      (framelessSumBC ?= scalaSumBC)
        .&&(framelessSumBCB ?= scalaSumBCB)
        .&&(framelessSumBCBC ?= scalaSumBCBC)
        .&&(framelessSumBCBCB ?= scalaSumBCBCB)
    }

    check(forAll(prop[String, Long, BigDecimal, Long, BigDecimal] _))
  }

  test("groupBy('a, 'b).agg(sum('c)) to groupBy('a, 'b).agg(sum('c),sum('c),sum('c),sum('c),sum('c))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering,
    C: TypedEncoder,
    OutC: TypedEncoder: Numeric
    ](data: List[X3[A, B, C]])(
      implicit
      summableC: CatalystSummable[C, OutC],
      widenc: C => OutC
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val framelessSumC = dataset
        .groupBy(A,B)
        .agg(sum(C))
        .collect().run.toVector.sortBy(x => (x._1,x._2))

      val scalaSumC = data.groupBy(x => (x.a,x.b)).mapValues { xs =>
        xs.map(_.c).map(widenc).sum
      }.toVector.map { case ((a, b), c) => (a, b, c) }.sortBy(x => (x._1,x._2))

      val framelessSumCC = dataset
        .groupBy(A,B)
        .agg(sum(C), sum(C))
        .collect().run.toVector.sortBy(x => (x._1,x._2))

      val scalaSumCC = data.groupBy(x => (x.a,x.b)).mapValues { xs =>
        val s = xs.map(_.c).map(widenc).sum; (s,s)
      }.toVector.map { case ((a, b), (c1, c2)) => (a, b, c1, c2) }.sortBy(x => (x._1,x._2))

      val framelessSumCCC = dataset
        .groupBy(A,B)
        .agg(sum(C), sum(C), sum(C))
        .collect().run.toVector.sortBy(x => (x._1,x._2))

      val scalaSumCCC = data.groupBy(x => (x.a,x.b)).mapValues { xs =>
        val s = xs.map(_.c).map(widenc).sum; (s,s,s)
      }.toVector.map { case ((a, b), (c1, c2, c3)) => (a, b, c1, c2, c3) }.sortBy(x => (x._1,x._2))

      val framelessSumCCCC = dataset
        .groupBy(A,B)
        .agg(sum(C), sum(C), sum(C), sum(C))
        .collect().run.toVector.sortBy(x => (x._1,x._2))

      val scalaSumCCCC = data.groupBy(x => (x.a,x.b)).mapValues { xs =>
        val s = xs.map(_.c).map(widenc).sum; (s,s,s,s)
      }.toVector.map { case ((a, b), (c1, c2, c3, c4)) => (a, b, c1, c2, c3, c4) }.sortBy(x => (x._1,x._2))

      val framelessSumCCCCC = dataset
        .groupBy(A,B)
        .agg(sum(C), sum(C), sum(C), sum(C), sum(C))
        .collect().run.toVector.sortBy(x => (x._1,x._2))

      val scalaSumCCCCC = data.groupBy(x => (x.a,x.b)).mapValues { xs =>
        val s = xs.map(_.c).map(widenc).sum; (s,s,s,s,s)
      }.toVector.map { case ((a, b), (c1, c2, c3, c4, c5)) => (a, b, c1, c2, c3, c4, c5) }.sortBy(x => (x._1,x._2))

      (framelessSumC ?= scalaSumC) &&
        (framelessSumCC ?= scalaSumCC) &&
        (framelessSumCCC ?= scalaSumCCC) &&
        (framelessSumCCCC ?= scalaSumCCCC) &&
        (framelessSumCCCCC ?= scalaSumCCCCC)
    }

    check(forAll(prop[String, Long, BigDecimal, BigDecimal] _))
  }

  test("groupBy('a, 'b).agg(sum('c), sum('d))") {
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
      summableD: CatalystSummable[D, OutD],
      widenc: C => OutC,
      widend: D => OutD
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)
      val D = dataset.col[D]('d)

      val datasetSumByAB = dataset
        .groupBy(A, B)
        .agg(sum(C), sum(D))
        .collect().run.toVector.sortBy(x => (x._1, x._2))

      val sumByAB = data.groupBy(x => (x.a, x.b)).mapValues { xs =>
        (xs.map(_.c).map(widenc).sum, xs.map(_.d).map(widend).sum)
      }.toVector.map {
        case ((a, b), (c, d)) => (a, b, c, d)
      }.sortBy(x => (x._1, x._2))

      datasetSumByAB ?= sumByAB
    }

    check(forAll(prop[Byte, Int, Long, BigDecimal, Long, BigDecimal] _))
  }

  test("groupBy('a, 'b).mapGroups('a, 'b, sum('c))") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder : Ordering,
      C: TypedEncoder : Numeric
    ](data: List[X3[A, B, C]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val datasetSumByAB = dataset
        .groupBy(A, B)
        .deserialized.mapGroups { case ((a, b), xs) => (a, b, xs.map(_.c).sum) }
        .collect().run().toVector.sortBy(x => (x._1, x._2))

      val sumByAB = data.groupBy(x => (x.a, x.b))
        .mapValues { xs => xs.map(_.c).sum }
        .toVector.map { case ((a, b), c) => (a, b, c) }.sortBy(x => (x._1, x._2))

      datasetSumByAB ?= sumByAB
    }

    check(forAll(prop[Byte, Int, Long] _))
  }

  test("groupBy('a).mapGroups(('a, toVector(('a, 'b))") {
    def prop[
      A: TypedEncoder: Ordering,
      B: TypedEncoder: Ordering
    ](data: Vector[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val datasetGrouped = dataset
        .groupBy(A)
        .deserialized.mapGroups((a, xs) => (a, xs.toVector.sorted))
        .collect().run.toMap

      val dataGrouped = data.groupBy(_.a).mapValues(_.sorted)

      datasetGrouped ?= dataGrouped
    }

    check(forAll(prop[Short, Option[Short]] _))
    check(forAll(prop[Option[Short], Short] _))
    check(forAll(prop[X1[Option[Short]], Short] _))
  }

  test("groupBy('a).flatMapGroups(('a, toVector(('a, 'b))") {
    def prop[
      A: TypedEncoder : Ordering,
      B: TypedEncoder : Ordering
    ](data: Vector[X2[A, B]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val datasetGrouped = dataset
        .groupBy(A)
        .deserialized.flatMapGroups((a, xs) => xs.map(x => (a, x)))
        .collect().run
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

  test("groupBy('a, 'b).flatMapGroups((('a,'b) toVector((('a,'b), 'c))") {
    def prop[
    A: TypedEncoder : Ordering,
    B: TypedEncoder : Ordering,
    C: TypedEncoder : Ordering
    ](data: Vector[X3[A, B, C]]): Prop = {
      val dataset = TypedDataset.create(data)
      val cA = dataset.col[A]('a)
      val cB = dataset.col[B]('b)

      val datasetGrouped = dataset
        .groupBy(cA, cB)
        .deserialized.flatMapGroups((a, xs) => xs.map(x => (a, x)))
        .collect().run()
        .sorted

      val dataGrouped = data
        .groupBy(t => (t.a,t.b)).toSeq
        .flatMap { case (a, xs) => xs.map(x => (a, x)) }
        .sorted

      datasetGrouped ?= dataGrouped
    }

    check(forAll(prop[Short, Option[Short], Long] _))
    check(forAll(prop[Option[Short], Short, Int] _))
    check(forAll(prop[X1[Option[Short]], Short, Byte] _))
  }
}
