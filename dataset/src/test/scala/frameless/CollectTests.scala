package frameless

import frameless.CollectTests.{ prop, propArray }
import org.apache.spark.sql.SparkSession
import org.scalacheck.Prop
import org.scalacheck.Prop._
import scala.reflect.ClassTag

class CollectTests extends TypedDatasetSuite {
  test("collect()") {
    check(forAll(propArray[Int] _))
    check(forAll(propArray[Long] _))
    check(forAll(propArray[Boolean] _))
    check(forAll(propArray[Float] _))
    check(forAll(propArray[String] _))
    check(forAll(propArray[Byte] _))
    check(forAll(propArray[Option[Int]] _))
    check(forAll(propArray[Option[Long]] _))
    check(forAll(propArray[Option[Double]] _))
    check(forAll(propArray[Option[Float]] _))
    check(forAll(propArray[Option[Short]] _))
    check(forAll(propArray[Option[Byte]] _))
    check(forAll(propArray[Option[Boolean]] _))
    check(forAll(propArray[Option[String]] _))

    check(forAll(prop[X2[Int, Int]] _))
    check(forAll(prop[X2[String, String]] _))
    check(forAll(prop[X2[String, Int]] _))
    check(forAll(prop[X2[Long, Int]] _))

    check(forAll(prop[X2[X2[Int, String], Boolean]] _))
    check(forAll(prop[Tuple1[Option[Int]]] _))

    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[String] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[Option[Int]] _))
    check(forAll(prop[Option[Long]] _))
    check(forAll(prop[Option[Double]] _))
    check(forAll(prop[Option[Float]] _))
    check(forAll(prop[Option[Short]] _))
    check(forAll(prop[Option[Byte]] _))
    check(forAll(prop[Option[Boolean]] _))
    check(forAll(prop[Option[String]] _))
    check(forAll(prop[Option[SQLDate]] _))
    check(forAll(prop[Option[SQLTimestamp]] _))

    check(forAll(prop[Vector[Int]] _))
    check(forAll(prop[List[Int]] _))
    check(forAll(prop[Seq[Int]] _))
    check(forAll(prop[Vector[Char]] _))
    check(forAll(prop[List[Char]] _))
    check(forAll(prop[Seq[Char]] _))
    check(forAll(prop[Seq[Seq[Seq[Char]]]] _))
    check(forAll(prop[Seq[Option[String]]] _))
    check(forAll(prop[Seq[Map[String, Long]]] _))
    check(forAll(prop[Seq[Map[String, X2[Option[Long], Vector[Boolean]]]]] _))
    check(forAll(prop[Option[Int]] _))
    check(forAll(prop[Vector[X2[Int, Int]]] _))

    check(forAll(prop[X1[Vector[Food]]] _))
    check(forAll(prop[X1[Vector[X1[Food]]]] _))
    check(forAll(prop[X1[Vector[X1[Int]]]] _))

    // TODO this doesn't work, and never worked...
    // check(forAll(prop[X1[Option[X1[Option[Int]]]]] _))

    check(forAll(prop[UdtEncodedClass] _))
    check(forAll(prop[Option[UdtEncodedClass]] _))
    check(forAll(prop[X1[UdtEncodedClass]] _))
    check(forAll(prop[X2[Int, UdtEncodedClass]] _))
    check(forAll(prop[(Long, UdtEncodedClass)] _))
  }
}

object CollectTests {
  import frameless.syntax._

  def prop[A: TypedEncoder : ClassTag](data: Vector[A])(implicit c: SparkSession): Prop =
    TypedDataset.create(data).collect().run().toVector ?= data

  def propArray[A: TypedEncoder : ClassTag](data: Vector[X1[Array[A]]])(implicit c: SparkSession): Prop =
    Prop(TypedDataset.create(data).collect().run().toVector.zip(data).forall {
      case (X1(l), X1(r)) => l.sameElements(r)
    })
}
