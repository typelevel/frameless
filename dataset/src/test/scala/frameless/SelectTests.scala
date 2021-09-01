package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped
import scala.reflect.ClassTag

class SelectTests extends TypedDatasetSuite {
  test("select('a) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val dataset2 = dataset.select(A).collect().run().toVector
      val data2 = data.map { case X4(a, _, _, _) => a }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[X2[Int, Int], Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[UdtEncodedClass, Int, Int, Int] _))
  }

  test("select('a, 'b) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        eab: TypedEncoder[(A, B)],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val dataset2 = dataset.select(A, B).collect().run().toVector
      val data2 = data.map { case X4(a, b, _, _) => (a, b) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, String, Int, Int] _))
  }

  test("select('a, 'b, 'c) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        eab: TypedEncoder[(A, B, C)],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val dataset2 = dataset.select(A, B, C).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, _) => (a, b, c) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, String, Int, Int] _))
  }

  test("select('a,'b,'c,'d) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4, a1).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a, 'c) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4, a1, a3).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a, c) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a,'c,'b) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4, a1, a3, a2).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a, c, b) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a,'c,'b, 'a) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4, a1, a3, a2, a1).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a, c, b, a) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a,'c,'b,'a,'c) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 = dataset.select(a1, a2, a3, a4, a1, a3, a2, a1, a3).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a, c, b, a, c) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a,'b,'c,'d,'a,'c,'b,'a,'c, 'd) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
        implicit ea: TypedEncoder[A],
        eb: TypedEncoder[B],
        ec: TypedEncoder[C],
        ed: TypedEncoder[D],
        ex4: TypedEncoder[X4[A, B, C, D]],
        ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val a1 = dataset.col[A]('a)
      val a2 = dataset.col[B]('b)
      val a3 = dataset.col[C]('c)
      val a4 = dataset.col[D]('d)

      val dataset2 =
        dataset.select(a1, a2, a3, a4, a1, a3, a2, a1, a3, a4).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, d) => (a, b, c, d, a, c, b, a, c, d) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, Boolean, Int, Float] _))
  }

  test("select('a.b)") {
    def prop[A, B, C](data: Vector[X2[X2[A, B], C]])(
        implicit eabc: TypedEncoder[X2[X2[A, B], C]],
        eb: TypedEncoder[B],
        cb: ClassTag[B]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val AB = dataset.colMany('a, 'b)

      val dataset2 = dataset.select(AB).collect().run().toVector
      val data2 = data.map { case X2(X2(_, b), _) => b }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, String, Double] _))
  }

  test("select with column expression addition") {
    def prop[A](data: Vector[X1[A]], const: A)(
        implicit eabc: TypedEncoder[X1[A]],
        anum: CatalystNumeric[A],
        num: Numeric[A],
        eb: TypedEncoder[A]
    ): Prop = {
      val ds = TypedDataset.create(data)

      val dataset2 = ds.select(ds('a) + const).collect().run().toVector
      val data2 = data.map { case X1(a) => num.plus(a, const) }

      dataset2 ?= data2
    }

    check(forAll(prop[Short] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
  }

  test("select with column expression multiplication") {
    def prop[A](data: Vector[X1[A]], const: A)(
        implicit eabc: TypedEncoder[X1[A]],
        anum: CatalystNumeric[A],
        num: Numeric[A],
        eb: TypedEncoder[A]
    ): Prop = {
      val ds = TypedDataset.create(data)

      val dataset2 = ds.select(ds('a) * const).collect().run().toVector
      val data2 = data.map { case X1(a) => num.times(a, const) }

      dataset2 ?= data2
    }

    check(forAll(prop[Short] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
  }

  test("select with column expression subtraction") {
    def prop[A](data: Vector[X1[A]], const: A)(
        implicit eabc: TypedEncoder[X1[A]],
        cnum: CatalystNumeric[A],
        num: Numeric[A],
        eb: TypedEncoder[A]
    ): Prop = {
      val ds = TypedDataset.create(data)

      val dataset2 = ds.select(ds('a) - const).collect().run().toVector
      val data2 = data.map { case X1(a) => num.minus(a, const) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
  }

  test("select with column expression division") {
    def prop[A](data: Vector[X1[A]], const: A)(
        implicit eabc: TypedEncoder[X1[A]],
        anum: CatalystNumeric[A],
        frac: Fractional[A],
        eb: TypedEncoder[A]
    ): Prop = {
      val ds = TypedDataset.create(data)

      if (const != 0) {
        val dataset2 =
          ds.select(ds('a) / const).collect().run().toVector.asInstanceOf[Vector[A]]
        val data2 = data.map { case X1(a) => frac.div(a, const) }
        dataset2 ?= data2
      } else 0 ?= 0
    }

    check(forAll(prop[Double] _))
  }

  test("tests to cover problematic dataframe column names during projections") {
    case class Foo(i: Int)
    val e = TypedDataset.create[Foo](Foo(1) :: Nil)
    val t: TypedDataset[(Int, Int)] = e.select(e.col('i) * 2, e.col('i))
    assert(t.select(t.col('_1)).collect().run().toList === List(2))
    // Issue #54
    val fooT = t.select(t.col('_1)).deserialized.map(x => Tuple1.apply(x)).as[Foo]
    assert(fooT.select(fooT('i)).collect().run().toList === List(2))
  }

  test("unary - on arithmetic") {
    val e =
      TypedDataset.create[(Int, String, Int)]((1, "a", 2) :: (2, "b", 4) :: (2, "b", 1) :: Nil)
    assert(e.select(-e('_1)).collect().run().toVector === Vector(-1, -2, -2))
    assert(e.select(-(e('_1) + e('_3))).collect().run().toVector === Vector(-3, -6, -3))
  }

  test("unary - on strings should not type check") {
    val e = TypedDataset.create[(Int, String, Long)](
      (1, "a", 2L) :: (2, "b", 4L) :: (2, "b", 1L) :: Nil)
    illTyped("""e.select( -e('_2) )""")
  }

  test("select with aggregation operations is not supported") {
    val e = TypedDataset.create[(Int, String, Long)](
      (1, "a", 2L) :: (2, "b", 4L) :: (2, "b", 1L) :: Nil)
    illTyped("""e.select(frameless.functions.aggregate.sum(e('_1)))""")
  }
}
