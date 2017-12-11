package frameless

import org.apache.spark.sql.FramelessInternals.UserDefinedType
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ functions => sfunc }
import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

@SQLUserDefinedType(udt = classOf[UdtMapEncoded])
class UdtMapClass(val a: Int) {
  override def equals(other: Any): Boolean = other match {
    case that: UdtMapClass => a == that.a
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[Any](a)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"UdtMapClass($a)"
}

object UdtMapClass {
  implicit val urtEncoderClass = new UdtMapEncoded
}

class UdtMapEncoded extends UserDefinedType[UdtMapClass] {
  override def sqlType: DataType = MapType(StringType, IntegerType)
  override def serialize(obj: UdtMapClass): Any =
    ArrayBasedMapData(Map.empty)
  override def deserialize(datum: Any): UdtMapClass =
    new UdtMapClass(1)
  override def userClass: Class[UdtMapClass] = classOf[UdtMapClass]
}

class SortTests extends TypedDatasetSuite {
  test("bad udt") {
    /*
    This is a UDT that from the outside looks like it only contains `Int`
    but internally is represented by a `MapType`. This means that the generic
    derivation will work from [[frameless.CatalystRowOrdered0]] but it'll blow
    up at runtime due `MapType` not being sortable!

    How can UDT be safely used here?
     */
    val ds = TypedDataset.create(Seq(Tuple1(new UdtMapClass(1))))

    /*This will blow up at runtime!!
      "due to data type mismatch: cannot sort data type udtmapencoded;"
    */
    ds.sort(ds('_1).asc).show().run()

    //regular spark also blow up at runtime!
    ds.dataset.sort(sfunc.col("_1"))
  }


  test("prevent sorting by Map") {
    val ds = TypedDataset.create(Seq(
      X2(1, Map.empty[String, Int])
    ))

    illTyped {
      """ds.sort(ds('d).desc)"""
    }
  }

  test("sorting") {
    def prop[A: TypedEncoder : CatalystRowOrdered](values: List[A]): Prop = {
      val input: List[X2[Int, A]] = values.zipWithIndex.map { case (a, i) => X2(i, a) }

      val ds = TypedDataset.create(input)

      (ds.sort(ds('b)).collect().run().toList ?= ds.dataset.sort(sfunc.col("b")).collect().toList) &&
        (ds.sort(ds('b).asc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc).collect().toList) &&
        (ds.sort(ds('b).desc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc).collect().toList)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(forAll(prop[List[String]] _))
    check(forAll(prop[List[X2[Int, X1[String]]]] _))
    check(forAll(prop[UdtEncodedClass] _))
  }

  test("sorting optional") {
    def prop[A: TypedEncoder : CatalystRowOrdered](values: List[Option[A]]): Prop = {
      val input: List[X2[Int, Option[A]]] = values.zipWithIndex.map { case (a, i) => X2(i, a) }

      val ds = TypedDataset.create(input)

      (ds.sort(ds('b)).collect().run().toList ?= ds.dataset.sort(sfunc.col("b")).collect().toList) &&
        (ds.sort(ds('b).asc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc).collect().toList) &&
        (ds.sort(ds('b).ascNonesFirst).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc_nulls_first).collect().toList) &&
        (ds.sort(ds('b).ascNonesLast).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc_nulls_last).collect().toList) &&
        (ds.sort(ds('b).desc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc).collect().toList) &&
        (ds.sort(ds('b).descNonesFirst).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc_nulls_first).collect().toList) &&
        (ds.sort(ds('b).descNonesLast).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc_nulls_last).collect().toList)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(forAll(prop[List[String]] _))
    check(forAll(prop[List[X2[Int, X1[String]]]] _))
  }
}
