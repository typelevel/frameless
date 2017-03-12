package frameless

import org.scalatest.Matchers
import shapeless.test.illTyped

case class Foo(a: Int, b: String) {
  // tests failure of unsupported unary operator
  def unary_~ : Foo = copy(a = a + 1)

  // tests failure of binary operator with different arg count (Column#when takes two args)
  def when(a: Int): Int = this.a + a
}

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

    val ds3 = ds.selectExpr(x => (x.a, x.c.length))
  }

  test("substitute length() on Array and Vector") {
    val ds = TypedDataset.create(Seq(
      X2(Array(1, 2, 3), Vector(1, 2))
    ))
    val ds2 = ds.selectExpr(x => (x.a.length, x.b.length))
    ds2.collect().run() should contain theSameElementsAs Seq((3, 2))
  }

  test("selectExpr constructing a case class") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))
    val ds2 = ds.selectExpr(x => X2(x.a, x.b))
    ds2.collect().run() should contain theSameElementsAs Seq(
      X2(10, 20.0),
      X2(20, 30.0)
    )
  }

  test("selectExpr constructing an unparameterized case class") {

    val ds = TypedDataset.create(Seq(Foo(10, "ten"), Foo(20, "twenty")))
    val ds2 = ds.selectExpr(f => Foo(f.a * 5, f.b + "foo"))
    ds2.collect().run() should contain theSameElementsAs Seq(
      Foo(50, "tenfoo"),
      Foo(100, "twentyfoo")
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

  test("selectExpr accessing a nested case class field") {
    val ds = TypedDataset.create(Seq(
      X2(X2(11.0, "foofoo"), 10.0),
      X2(X2(22.0, "barfoo"), 15.0)
    ))

    val ds2 = ds.selectExpr(x => (x.a.a * x.b, x.a.b))

    ds2.collect().run() should contain theSameElementsAs Seq(
      (110.0, "foofoo"),
      (330.0, "barfoo")
    )
  }

  test("can't use a non-constructor apply method") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))

    object Foo {
      def apply(str: String, i: Int): String = str + i.toString
    }

    object FooP {
      def apply[A, B](a: A, b: B): X2[A, B] = X2(a, b)
    }

    illTyped(
      "val ds2 = ds.selectExpr(x => Foo(x.c, x.a))",
      ".*Only constructor can be used here"
    )

    illTyped(
      "val ds2 = ds.selectExpr(x => FooP(x.c, x.a))",
      ".*Only constructor can be used here"
    )

  }

  test("quoted functions expanded to spark native column functions") {
    import frameless.functions.quoted._
    val ds = TypedDataset.create(Seq(X4(10, 20.0, "foo", 10L), X4(-20, 30.0, "bar", 20L)))

    def test[U : TypedEncoder](expected: U*)(fn: TypedDataset[X4[Int, Double, String, Long]] => TypedDataset[U]) =
      fn(ds).collect().run() should contain theSameElementsAs expected

    def testConv[T, U : TypedEncoder](expected: T*)(fn: TypedDataset[X4[Int, Double, String, Long]] => TypedDataset[U])(conv: U => T) =
      fn(ds).collect().run().map(conv) should contain theSameElementsAs expected

    test(10, 20)(_.selectExpr(x => abs(x.a)))
    testConv(List(10, 10, 10), List(-20, -20, -20))(_.selectExpr(x => array(x.a, x.a, x.a)))(_.toList)
    test(Map("foo" -> 10), Map("bar" -> -20))(_.selectExpr(x => map(x.c -> x.a)))
    test("foo", "bar")(_.selectExpr(x => coalesce(null, x.c)))
    test(("foo", ""), ("bar", ""))(_.selectExpr(x => (x.c, input_file_name())))
    test((false, true), (false, true))(_.selectExpr(x => (isnan(x.b), isnan(Double.NaN))))
    test((false, true), (false, true))(_.selectExpr(x => (isnull(x.c), isnull(null: String))))
    test(0L, 1L)(_.selectExpr(x => monotonically_increasing_id()))
    test(20.0, 30.0)(_.selectExpr(x => nanvl(Double.NaN, x.b)))
    test(-10, 20)(_.selectExpr(x => negate(x.a)))

    // no way to compare output, but test to make sure the functions can be executed
    ds.selectExpr(x => (x.a, rand(10L), rand(), randn(10L), randn())).collect().run()

    test(math.sqrt(20.0), math.sqrt(30.0))(_.selectExpr(x => sqrt(x.b)))
    test("ten", "not ten")(_.selectExpr(x => when(x.a == 10, "ten") otherwise "not ten"))
    test(~10, ~(-20))(_.selectExpr(x => bitwiseNOT(x.a)))
    test(
      (math.acos(20.0 / 32.0), math.asin(20.0 / 32.0), math.atan(20.0), math.atan2(20.0, 20.0)),
      (math.acos(30.0 / 32.0), math.asin(30.0 / 32.0), math.atan(30.0), math.atan2(30.0, 30.0))
    )(_.selectExpr(x => (acos(x.b / 32), asin(x.b / 32), atan(x.b), atan2(x.b, x.b))))
    test(java.lang.Long.toBinaryString(10L), java.lang.Long.toBinaryString(20L))(_.selectExpr(x => bin(x.d)))

    // aggregate functions
    test(2L)(_.selectExpr(x => count(x.a)))
    test(2L)(_.selectExpr(x => countDistinct(x.a)))
    test(2L)(_.selectExpr(x => countDistinct(x.a, x.b)))

    illTyped("val a: Int = 22.22")
  }

  //TODO: could we just UDF the function in this case?
  test("arbitrary functions not yet supported") {
    def strfun(s: String) = s"fun${s}fun"

    object Fun {
      def strfun(s: String) = s"Funfun${s}Funfun"
    }

    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))

    illTyped(
      "val ds2 = ds.selectExpr(x => (x.a, strfun(x.c)))",
      ".*Function application not currently supported"
    )

    illTyped(
      "val ds2 = ds.selectExpr(x => (x.a, Fun.strfun(x.c)))",
      ".*Function application not currently supported"
    )
  }

  test("fails if operator is not available on Column") {
    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))

    illTyped(
      "val ds2 = ds.selectExpr(x => x.c substring x.a)",
      ".*substring is not a valid column operator"
    )

    val ds3 = TypedDataset.create(Seq(X2(Foo(10, "ten"), "foo"), X2(Foo(20, "twenty"), "bar")))

    illTyped(
      "val ds4 = ds3.selectExpr(x => ~x.a)",
      ".*~ is not a valid column operator"
    )

    illTyped(
      """val ds5 = ds3.selectExpr(x => x.a when 5)""",
      ".*when is not a valid column operator"
      // would be nice if this error was clearer about the arity mismatch being the cause
    )
  }

  test("multiple errors are all shown before aborting") {
    def strfun(s: String) = s"fun${s}fun"

    object Fun {
      def strfun(s: String) = s"Funfun${s}Funfun"
    }

    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))

    illTyped(
      "val ds2 = ds.selectExpr(x => (x.a, (strfun(x.c), Fun.strfun(x.c))))",
      ".*Function application not currently supported"
      // this is a limitation of illTyped - it only looks at the first emitted error, rather than the error that aborted
    )
  }

  test("errors emitted from nested expressions in allowed binary operator") {
    // tests failFrom case in binary operator case

    def intfun(i: Int): Int = i + 1

    val ds = TypedDataset.create(Seq(X3(10, 20.0, "foo"), X3(20, 30.0, "bar")))

    illTyped(
      "val ds2 = ds.selectExpr(x => (x.c, x.a + intfun(x.a)))",
      ".*Function application not currently supported"
    )

  }
}
