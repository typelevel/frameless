package frameless

import frameless.functions._
import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class GroupByTests extends TypedDatasetSuite {
  // Datasets are coalesced due to https://issues.apache.org/jira/browse/SPARK-12675

  test("groupBy('a).agg(sum('b))") {
    def prop[A, B](data: List[X2[A, B]])(
      implicit
      ea: TypedEncoder[A],
      eb: TypedEncoder[B],
      ex2: TypedEncoder[X2[A, B]],
      et2: TypedEncoder[(A, B)],
      n: Numeric[B],
      s: Summable[B],
      o: Ordering[A] // to compare ordered vectors
    ): Prop = {
      val dataset = TypedDataset.create(data).coalesce(2)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val datasetSumByA = dataset.groupBy(A).agg(sum(B, n.fromInt(0))).collect().run().toVector.sortBy(_._1)
      val sumByA = data.groupBy(_.a).mapValues(_.map(_.b).sum).toVector.sortBy(_._1)

      datasetSumByA ?= sumByA
    }

    check {forAll { (xs: List[X2[Int, Long]]) => prop(xs) }}
  }

  test("groupBy('a).agg(sum('b), sum('c))") {
    def prop[A, B, C](data: List[X3[A, B, C]])(
      implicit
      ea: TypedEncoder[A],
      eb: TypedEncoder[B],
      ec: TypedEncoder[C],
      ex3: TypedEncoder[X3[A, B, C]],
      et3: TypedEncoder[(A, B, C)],
      nb: Numeric[B],
      nc: Numeric[C],
      sb: Summable[B],
      sc: Summable[C],
      o: Ordering[A] // to compare ordered vectors
    ): Prop = {
      val dataset = TypedDataset.create(data).coalesce(2)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val datasetSumByAB = dataset
        .groupBy(A)
        .agg(sum(B, nb.fromInt(0)), sum(C, nc.fromInt(0)))
        .collect().run().toVector.sortBy(_._1)

      val sumByAB = data.groupBy(_.a).mapValues { xs =>
        (xs.map(_.b).sum, xs.map(_.c).sum)
      }.toVector.map {
        case (a, (b, c)) => (a, b, c)
      }.sortBy(_._1)

      datasetSumByAB ?= sumByAB
    }

    check {forAll { (xs: List[X3[String, Long, BigDecimal]]) => prop(xs) }}
  }

  test("groupBy('a, 'b).agg(sum('c), sum('d))") {
    def prop[A, B, C, D](data: List[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      eb: TypedEncoder[B],
      ec: TypedEncoder[C],
      ed: TypedEncoder[D],
      ex3: TypedEncoder[X4[A, B, C, D]],
      et4: TypedEncoder[(A, B, C, D)],
      nc: Numeric[C],
      nd: Numeric[D],
      sc: Summable[C],
      sd: Summable[D],
      o: Ordering[(A, B)] // to compare ordered vectors
    ): Prop = {
      val dataset = TypedDataset.create(data).coalesce(2)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)
      val D = dataset.col[D]('d)

      val datasetSumByAB = dataset
        .groupBy(A, B)
        .agg(sum(C, nc.fromInt(0)), sum(D, nd.fromInt(0)))
        .collect().run().toVector.sortBy(x => (x._1, x._2))

      val sumByAB = data.groupBy(x => (x.a, x.b)).mapValues { xs =>
        (xs.map(_.c).sum, xs.map(_.d).sum)
      }.toVector.map {
        case ((a, b), (c, d)) => (a, b, c, d)
      }.sortBy(x => (x._1, x._2))

      datasetSumByAB ?= sumByAB
    }

    check {forAll { (xs: List[X4[Byte, Int, Long, BigDecimal]]) => prop(xs) }}
  }
}
