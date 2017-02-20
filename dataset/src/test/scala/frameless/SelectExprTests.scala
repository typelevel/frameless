package frameless

import org.scalatest.Matchers

class SelectExprTests extends TypedDatasetSuite with Matchers {

  test("selectExpr with a single column") {
    val ds = TypedDataset.create(Seq(X2(20, "twenty"), X2(30, "thirty")))
    val ds2 = ds.selectExpr(_.a)
    val ds3 = ds.selectExpr(_.b)
    ds2.collect().run() should contain theSameElementsAs Seq(20, 30)
    ds3.collect().run() should contain theSameElementsAs Seq("twenty", "thirty")
  }

  test("selectExpr with a single column and unary operation") {
    val ds = TypedDataset.create(Seq(X1(20), X1(30)))
    val ds2 = ds.selectExpr(-_.a)
    ds2.collect().run() should contain theSameElementsAs Seq(-20, -30)
  }

  test("selectExpr with a binary operation between two columns") {
    val ds = TypedDataset.create(Seq(X2(10, 20), X2(20, 30)))
    val ds2 = ds.selectExpr(x => x.a * x.b)
    ds2.collect().run() should contain theSameElementsAs Seq(200, 600)
  }

  test("selectExpr with a binary operation between a column and a literal") {
    val ds = TypedDataset.create(Seq(X2(10, 20), X2(20, 30)))
    val ds2 = ds.selectExpr(x => x.a * 10)
    ds2.collect().run() should contain theSameElementsAs Seq(100, 200)
  }

  test("selectExpr constructing a tuple") {
    val ds = TypedDataset.create(Seq(X2(10, 20), X2(20, 30)))
    val ds2 = ds.selectExpr(x => (x.a, x.b))
    ds2.collect().run() should contain theSameElementsAs Seq((10, 20), (20, 30))
  }

  test("selectExpr constructing a tuple with an operation") {
    val ds = TypedDataset.create(Seq(X2(10, 20), X2(20, 30)))
    val ds2 = ds.selectExpr(x => (x.a * 10, x.b + 1))
    ds2.collect().run() should contain theSameElementsAs Seq((100, 21), (200, 31))
  }

  test("selectExpr constructing a nested tuple with substitute function") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))
    val ds2 = ds.selectExpr(x => ((x.a * 1.1, x.c + "foo"), x.b / 2))
    ds2.collect().run() should contain theSameElementsAs Seq(
      ((11.0, "foofoo"), 10.0),
      ((22.0, "barfoo"), 15.0)
    )
  }

  test("selectExpr constructing a case class") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))
    val ds2 = ds.selectExpr(x => X2(x.a, x.b))
    ds2.collect().run() should contain theSameElementsAs Seq(
      X2(10, 20.0),
      X2(20, 30.0)
    )
  }

  test("selectExpr constructing a nested case class") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))
    val ds2 = ds.selectExpr(x => X2(X2(x.a, x.b), x.c))
    ds2.collect().run() should contain theSameElementsAs Seq(
      X2(X2(10, 20.0), "foo"),
      X2(X2(20, 30.0), "bar")
    )
  }
}
