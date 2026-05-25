package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

class DropTest extends TypedDatasetSuite {
  import DropTest._

  test("fail to compile on missing value") {
    val f: TypedDataset[X] = TypedDataset.create(
      X(1, 1, false) :: X(1, 1, false) :: X(1, 10, false) :: Nil
    )
    illTyped {
      """val fNew: TypedDataset[XMissing] = f.drop[XMissing]('j)"""
    }
  }

  test("fail to compile on different column name") {
    val f: TypedDataset[X] = TypedDataset.create(
      X(1, 1, false) :: X(1, 1, false) :: X(1, 10, false) :: Nil
    )
    illTyped {
      """val fNew: TypedDataset[XDifferentColumnName] = f.drop[XDifferentColumnName]('j)"""
    }
  }

  test("fail to compile on added column name") {
    val f: TypedDataset[X] = TypedDataset.create(
      X(1, 1, false) :: X(1, 1, false) :: X(1, 10, false) :: Nil
    )
    illTyped {
      """val fNew: TypedDataset[XAdded] = f.drop[XAdded]('j)"""
    }
  }

  test("remove column in the middle") {
    val f: TypedDataset[X] = TypedDataset.create(
      X(1, 1, false) :: X(1, 1, false) :: X(1, 10, false) :: Nil
    )
    val fNew: TypedDataset[XGood] = f.drop[XGood]

    fNew.collect().run().foreach(xg => assert(xg === XGood(1, false)))
  }

  test("drop four columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d5 = TypedDataset.create(X5(value, value, value, value, value) :: Nil)
      val d4 = d5.drop[X4[A, A, A, A]]
      val d3 = d4.drop[X3[A, A, A]]
      val d2 = d3.drop[X2[A, A]]
      val d1 = d2.drop[X1[A]]

      X1(value) ?= d1.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}

object DropTest {
  case class X(i: Int, j: Int, k: Boolean)
  case class XMissing(i: Int)
  case class XDifferentColumnName(ij: Int, k: Boolean)
  case class XAdded(i: Int, j: Int, k: Boolean, l: Int)
  case class XGood(i: Int, k: Boolean)
}
