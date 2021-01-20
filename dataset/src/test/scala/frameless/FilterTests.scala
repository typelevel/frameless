package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FilterTests extends TypedDatasetSuite {
  test("filter('a == lit(b))") {
    def prop[A: TypedEncoder](elem: A, data: Vector[X1[A]])(implicit ex1: TypedEncoder[X1[A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)

      val dataset2 = dataset.filter(A === elem).collect().run().toVector
      val data2 = data.filter(_.a == elem)

      dataset2 ?= data2
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("filter('a =!= lit(b))") {
    def prop[A: TypedEncoder](elem: A, data: Vector[X1[A]])(implicit ex1: TypedEncoder[X1[A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)

      val dataset2 = dataset.filter(A =!= elem).collect().run().toVector
      val data2 = data.filter(_.a != elem)

      dataset2 ?= data2
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[Vector[SQLTimestamp]] _))
  }

  test("filter('a =!= 'b)") {
    def prop[A: TypedEncoder](data: Vector[X2[A, A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)
      val B = dataset.col('b)

      val dataset2 = dataset.filter(A =!= B).collect().run().toVector
      val data2 = data.filter(x => x.a != x.b)

      dataset2 ?= data2
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[Vector[SQLTimestamp]] _))
  }

  test("filter('a =!= 'b") {
    def prop[A: TypedEncoder](elem: A, data: Vector[X2[A,A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val cA = dataset.col('a)
      val cB = dataset.col('b)

      val dataset2 = dataset.filter(cA =!= cB).collect().run().toVector
      val data2 = data.filter(x => x.a != x.b )

      (dataset2 ?= data2).&&(dataset.filter(cA =!= cA).count().run() ?= 0)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[Vector[SQLTimestamp]] _))
  }

  test("filter with arithmetic expressions: addition") {
    check(forAll { (data: Vector[X1[Int]]) =>
      val ds = TypedDataset.create(data)
      val res = ds.filter((ds('a) + 1) === (ds('a) + 1)).collect().run().toVector
      res ?= data
    })
  }

  test("filter with values (not columns): addition") {
    check(forAll { (data: Vector[X1[Int]], const: Int) =>
      val ds = TypedDataset.create(data)
      val res = ds.filter(ds('a) > const).collect().run().toVector
      res ?= data.filter(_.a > const)
    })
  }

  test("filter with arithmetic expressions: multiplication") {
    val t = X1(1) :: X1(2) :: X1(3) :: Nil
    val tds: TypedDataset[X1[Int]] = TypedDataset.create(t)

    assert(tds.filter(tds('a) * 2 === 2).collect().run().toVector === Vector(X1(1)))
    assert(tds.filter(tds('a) * 3 === 3).collect().run().toVector === Vector(X1(1)))
  }

  test("Option equality/inequality for columns") {
    def prop[A <: Option[_] : TypedEncoder](a: A, b: A): Prop = {
      val data = X2(a, b) :: X2(a, a) :: Nil
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)
      val B = dataset.col('b)

      (data.filter(x => x.a == x.b).toSet ?= dataset.filter(A === B).collect().run().toSet).
        &&(data.filter(x => x.a != x.b).toSet ?= dataset.filter(A =!= B).collect().run().toSet).
        &&(data.filter(x => x.a == None).toSet ?= dataset.filter(A.isNone).collect().run().toSet).
        &&(data.filter(x => x.a == None).toSet ?= dataset.filter(A.isNotNone === false).collect().run().toSet)
    }

    check(forAll(prop[Option[Int]] _))
    check(forAll(prop[Option[Boolean]] _))
    check(forAll(prop[Option[SQLDate]] _))
    check(forAll(prop[Option[SQLTimestamp]] _))
    check(forAll(prop[Option[X1[String]]] _))
    check(forAll(prop[Option[X1[X1[String]]]] _))
    check(forAll(prop[Option[X1[X1[Vector[Option[Int]]]]]] _))
  }

  test("Option equality/inequality for lit") {
    def prop[A <: Option[_] : TypedEncoder](a: A, b: A, cLit: A): Prop = {
      val data = X2(a, b) :: X2(a, cLit) :: Nil
      val dataset = TypedDataset.create(data)
      val colA = dataset.col('a)

      (data.filter(x => x.a == cLit).toSet ?= dataset.filter(colA === cLit).collect().run().toSet).
        &&(data.filter(x => x.a != cLit).toSet ?= dataset.filter(colA =!= cLit).collect().run().toSet).
        &&(data.filter(x => x.a == None).toSet ?= dataset.filter(colA.isNone).collect().run().toSet).
        &&(data.filter(x => x.a == None).toSet ?= dataset.filter(colA.isNotNone === false).collect().run().toSet)
    }

    check(forAll(prop[Option[Int]] _))
    check(forAll(prop[Option[Boolean]] _))
    check(forAll(prop[Option[SQLDate]] _))
    check(forAll(prop[Option[SQLTimestamp]] _))
    check(forAll(prop[Option[String]] _))
    check(forAll(prop[Option[X1[String]]] _))
    check(forAll(prop[Option[X1[X1[String]]]] _))
    check(forAll(prop[Option[X1[X1[Vector[Option[Int]]]]]] _))
  }

  test("filter with isin values") {
    def prop[A: TypedEncoder](data: Vector[X1[A]], values: Vector[A])(implicit a : CatalystIsin[A]): Prop = {
      val ds = TypedDataset.create(data)
      val res = ds.filter(ds('a).isin(values:_*)).collect().run().toVector
      res ?= data.filter(d => values.contains(d.a))
    }

    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[String] _))
  }
}
