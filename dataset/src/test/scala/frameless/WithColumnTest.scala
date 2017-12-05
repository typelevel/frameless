package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

class WithColumnTest extends TypedDatasetSuite {
  import WithColumnTest._

  test("fail to compile on missing value") {
    val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    illTyped {
      """val fNew: TypedDataset[XMissing] = f.withColumn[XMissing](f('j) === 10)"""
    }
  }

  test("fail to compile on different column name") {
    val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    illTyped {
      """val fNew: TypedDataset[XDifferentColumnName] = f.withColumn[XDifferentColumnName](f('j) === 10)"""
    }
  }

  test("fail to compile on added column name") {
    val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    illTyped {
      """val fNew: TypedDataset[XAdded] = f.withColumn[XAdded](f('j) === 10)"""
    }
  }

  test("fail to compile on wrong typed column") {
    val f: TypedDataset[X] = TypedDataset.create(X(1,1) :: X(1,1) :: X(1,10) :: Nil)
    illTyped {
      """val fNew: TypedDataset[XWrongType] = f.withColumn[XWrongType](f('j) === 10)"""
    }
  }

  test("append four columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d = TypedDataset.create(X1(value) :: Nil)
      val d1 = d.withColumn[X2[A, A]](d('a))
      val d2 = d1.withColumn[X3[A, A, A]](d1('b))
      val d3 = d2.withColumn[X4[A, A, A, A]](d2('c))
      val d4 = d3.withColumn[X5[A, A, A, A, A]](d3('d))

      X5(value, value, value, value, value) ?= d4.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }

  test("update in place") {
    def prop[A : TypedEncoder](startValue: A, replaceValue: A): Prop = {
      val d = TypedDataset.create(X2(startValue, replaceValue) :: Nil)

      val X2(a, b) = d.withColumn('a, d('b))
        .collect()
        .run()
        .head

      a ?= b
    }
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}

object WithColumnTest {
  case class X(i: Int, j: Int)
  case class XMissing(i: Int, k: Boolean)
  case class XDifferentColumnName(i: Int, ji: Int, k: Boolean)
  case class XAdded(i: Int, j: Int, k: Boolean, l: Int)
  case class XWrongType(i: Int, j: Int, k: Int)
  case class XGood(i: Int, j: Int, k: Boolean)
}
