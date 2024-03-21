package frameless
package functions

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.collection.immutable.{ ListSet, TreeSet }

class UdfTests extends TypedDatasetSuite {

  test("one argument udf") {
    evalCodeGens {
      def prop[A: TypedEncoder, B: TypedEncoder](
          data: Vector[X1[A]],
          f1: A => B
        ): Prop = {
        val dataset: TypedDataset[X1[A]] = TypedDataset.create(data)
        val u1 = udf[X1[A], A, B](f1)
        val u2 = dataset.makeUDF(f1)
        val A = dataset.col[A]('a)

        // filter forces whole codegen
        val codegen = dataset.deserialized
          .filter((_: X1[A]) => true)
          .select(u1(A))
          .collect()
          .run()
          .toVector

        // otherwise it uses local relation
        val local = dataset.select(u2(A)).collect().run().toVector

        val d = data.map(x => f1(x.a))

        (codegen ?= d) && (local ?= d)
      }

      check(forAll(prop[Int, Int] _))
      check(forAll(prop[String, String] _))
      check(forAll(prop[Option[Int], Option[Int]] _))
      check(forAll(prop[X1[Int], X1[Int]] _))
      check(forAll(prop[X1[Option[Int]], X1[Option[Int]]] _))

      // TODO doesn't work for the same reason as `collect`
      // check(forAll(prop[X1[Option[X1[Int]]], X1[Option[X1[Option[Int]]]]] _))

      // Vector/List isn't supported by MapObjects, not all collections are equal see #804
      check(forAll(prop[Option[Seq[String]], Option[Seq[String]]] _))
      check(forAll(prop[Option[List[String]], Option[List[String]]] _))
      check(forAll(prop[Option[Vector[String]], Option[Vector[String]]] _))

      // ListSet/TreeSet weren't supported before #804
      check(forAll(prop[Option[Set[String]], Option[Set[String]]] _))
      check(forAll(prop[Option[ListSet[String]], Option[ListSet[String]]] _))
      check(forAll(prop[Option[TreeSet[String]], Option[TreeSet[String]]] _))

      def prop2[A: TypedEncoder, B: TypedEncoder](f: A => B)(a: A): Prop =
        prop(Vector(X1(a)), f)

      check(
        forAll(
          prop2[Int, Option[Int]](x => if (x % 2 == 0) Some(x) else None) _
        )
      )
      check(forAll(prop2[Option[Int], Int](x => x getOrElse 0) _))
    }
  }

  test("multiple one argument udf") {
    evalCodeGens {
      def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
          data: Vector[X3[A, B, C]],
          f1: A => A,
          f2: B => B,
          f3: C => C
        ): Prop = {
        val dataset = TypedDataset.create(data)
        val u11 = udf[X3[A, B, C], A, A](f1)
        val u21 = udf[X3[A, B, C], B, B](f2)
        val u31 = udf[X3[A, B, C], C, C](f3)
        val u12 = dataset.makeUDF(f1)
        val u22 = dataset.makeUDF(f2)
        val u32 = dataset.makeUDF(f3)
        val A = dataset.col[A]('a)
        val B = dataset.col[B]('b)
        val C = dataset.col[C]('c)

        val dataset21 =
          dataset.select(u11(A), u21(B), u31(C)).collect().run().toVector
        val dataset22 =
          dataset.select(u12(A), u22(B), u32(C)).collect().run().toVector
        val d = data.map(x => (f1(x.a), f2(x.b), f3(x.c)))

        (dataset21 ?= d) && (dataset22 ?= d)
      }

      check(forAll(prop[Int, Int, Int] _))
      check(forAll(prop[String, Int, Int] _))
      check(forAll(prop[X3[Int, String, Boolean], Int, Int] _))
      check(forAll(prop[X3U[Int, String, Boolean], Int, Int] _))
    }
  }

  test("two argument udf") {
    evalCodeGens {
      def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
          data: Vector[X3[A, B, C]],
          f1: (A, B) => C
        ): Prop = {
        val dataset = TypedDataset.create(data)
        val u1 = udf[X3[A, B, C], A, B, C](f1)
        val u2 = dataset.makeUDF(f1)
        val A = dataset.col[A]('a)
        val B = dataset.col[B]('b)

        val dataset21 = dataset.select(u1(A, B)).collect().run().toVector
        val dataset22 = dataset.select(u2(A, B)).collect().run().toVector
        val d = data.map(x => f1(x.a, x.b))

        (dataset21 ?= d) && (dataset22 ?= d)
      }

      check(forAll(prop[Int, Int, Int] _))
      check(forAll(prop[String, Int, Int] _))
    }
  }

  test("multiple two argument udf") {
    evalCodeGens {
      def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
          data: Vector[X3[A, B, C]],
          f1: (A, B) => C,
          f2: (B, C) => A
        ): Prop = {
        val dataset = TypedDataset.create(data)
        val u11 = udf[X3[A, B, C], A, B, C](f1)
        val u12 = dataset.makeUDF(f1)
        val u21 = udf[X3[A, B, C], B, C, A](f2)
        val u22 = dataset.makeUDF(f2)

        val A = dataset.col[A]('a)
        val B = dataset.col[B]('b)
        val C = dataset.col[C]('c)

        val dataset21 =
          dataset.select(u11(A, B), u21(B, C)).collect().run().toVector
        val dataset22 =
          dataset.select(u12(A, B), u22(B, C)).collect().run().toVector
        val d = data.map(x => (f1(x.a, x.b), f2(x.b, x.c)))

        (dataset21 ?= d) && (dataset22 ?= d)
      }

      check(forAll(prop[Int, Int, Int] _))
      check(forAll(prop[String, Int, Int] _))
    }
  }

  test("three argument udf") {
    evalCodeGens {
      forceInterpreted {
        def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
            data: Vector[X3[A, B, C]],
            f: (A, B, C) => C
          ): Prop = {
          val dataset = TypedDataset.create(data)
          val u1 = udf[X3[A, B, C], A, B, C, C](f)
          val u2 = dataset.makeUDF(f)

          val A = dataset.col[A]('a)
          val B = dataset.col[B]('b)
          val C = dataset.col[C]('c)

          val dataset21 = dataset.select(u1(A, B, C)).collect().run().toVector
          val dataset22 = dataset.select(u2(A, B, C)).collect().run().toVector
          val d = data.map(x => f(x.a, x.b, x.c))

          (dataset21 ?= d) && (dataset22 ?= d)
        }

        check(forAll(prop[Int, Int, Int] _))
        check(forAll(prop[String, Int, Int] _))
      }
    }
  }

  test("four argument udf") {
    evalCodeGens {
      forceInterpreted {
        def prop[
            A: TypedEncoder,
            B: TypedEncoder,
            C: TypedEncoder,
            D: TypedEncoder
          ](data: Vector[X4[A, B, C, D]],
            f: (A, B, C, D) => C
          ): Prop = {
          val dataset = TypedDataset.create(data)
          val u1 = udf[X4[A, B, C, D], A, B, C, D, C](f)
          val u2 = dataset.makeUDF(f)

          val A = dataset.col[A]('a)
          val B = dataset.col[B]('b)
          val C = dataset.col[C]('c)
          val D = dataset.col[D]('d)

          val dataset21 =
            dataset.select(u1(A, B, C, D)).collect().run().toVector
          val dataset22 =
            dataset.select(u2(A, B, C, D)).collect().run().toVector
          val d = data.map(x => f(x.a, x.b, x.c, x.d))

          (dataset21 ?= d) && (dataset22 ?= d)
        }

        check(forAll(prop[Int, Int, Int, Int] _))
        check(forAll(prop[String, Int, Int, String] _))
        check(forAll(prop[String, String, String, String] _))
        check(forAll(prop[String, Long, String, String] _))
        check(forAll(prop[String, Boolean, Boolean, String] _))
      }
    }
  }

  test("five argument udf") {
    evalCodeGens {
      forceInterpreted {
        def prop[
            A: TypedEncoder,
            B: TypedEncoder,
            C: TypedEncoder,
            D: TypedEncoder,
            E: TypedEncoder
          ](data: Vector[X5[A, B, C, D, E]],
            f: (A, B, C, D, E) => C
          ): Prop = {
          val dataset = TypedDataset.create(data)
          val u1 = udf[X5[A, B, C, D, E], A, B, C, D, E, C](f)
          val u2 = dataset.makeUDF(f)

          val A = dataset.col[A]('a)
          val B = dataset.col[B]('b)
          val C = dataset.col[C]('c)
          val D = dataset.col[D]('d)
          val E = dataset.col[E]('e)

          val dataset21 =
            dataset.select(u1(A, B, C, D, E)).collect().run().toVector
          val dataset22 =
            dataset.select(u2(A, B, C, D, E)).collect().run().toVector
          val d = data.map(x => f(x.a, x.b, x.c, x.d, x.e))

          (dataset21 ?= d) && (dataset22 ?= d)
        }

        check(forAll(prop[Int, Int, Int, Int, Int] _))
      }
    }
  }
}
