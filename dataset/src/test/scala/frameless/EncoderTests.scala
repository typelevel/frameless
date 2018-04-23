package frameless

import java.util.UUID

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, UnresolvedMapObjects}
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression, UpCast}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalactic.Equality
import org.scalatest.Matchers

class EncoderTests extends TypedDatasetSuite with Matchers {

  private implicit val expressionSeqEquality: Equality[Seq[Expression]] = new Equality[Seq[Expression]] {
    override def areEqual(a: Seq[Expression], xb: Any): Boolean = {
      val b = xb.asInstanceOf[Seq[Expression]]
      a zip b map { case (a, b) => expressionEquality.areEquivalent(a, b) } reduce (_ && _)
    }
  }

  private val fixedExprId = new ExprId(0, UUID.randomUUID)

  private val simplify: PartialFunction[Expression, Expression] = {
    case x: Alias => x.copy()(fixedExprId, x.qualifier, x.explicitMetadata)
    case x: AssertNotNull => x.child // FIXME add not-null assertions?
    case x: UpCast => x.child // FIXME add upcasts?
    case x: UnresolvedMapObjects => x.child // FIXME add upcasts and assertions for each element?
  }

  private implicit val expressionEquality: Equality[Expression] = new Equality[Expression] {
    override def areEqual(a: Expression, xb: Any): Boolean = {
      val b = xb.asInstanceOf[Expression]
      val ta = a.transformUp(simplify)
      val tb = b.transformUp(simplify)
      ta == tb
    }
  }

  private def assertHasCorrectEncoder[T : Encoder : TypedEncoder] = {
    val sparkEncoder = implicitly[Encoder[T]].asInstanceOf[ExpressionEncoder[T]]
    val framelessEncoder = TypedExpressionEncoder[T]

    // FIXME option types are breaking this constraint
    framelessEncoder.flat shouldEqual sparkEncoder.flat
    framelessEncoder.schema shouldEqual sparkEncoder.schema
    framelessEncoder.serializer shouldEqual sparkEncoder.serializer
    framelessEncoder.deserializer shouldEqual sparkEncoder.deserializer
  }

  test("Primitive types have correct schema") {
    val spark = implicitly[SparkSession]
    import spark.implicits._

    assertHasCorrectEncoder[String]
    assertHasCorrectEncoder[BigDecimal]
    assertHasCorrectEncoder[java.math.BigDecimal]
    assertHasCorrectEncoder[Int]
    assertHasCorrectEncoder[Long]
    assertHasCorrectEncoder[Double]
    assertHasCorrectEncoder[Float]
    assertHasCorrectEncoder[Short]
    assertHasCorrectEncoder[Byte]
    assertHasCorrectEncoder[Boolean]
  }

  test("Option types have correct schema") {
    val spark = implicitly[SparkSession]
    import spark.implicits._

    assertHasCorrectEncoder[Option[String]]
    assertHasCorrectEncoder[Option[BigDecimal]]
    assertHasCorrectEncoder[Option[java.math.BigDecimal]]
    assertHasCorrectEncoder[Option[Int]]
    assertHasCorrectEncoder[Option[Long]]
    assertHasCorrectEncoder[Option[Double]]
    assertHasCorrectEncoder[Option[Float]]
    assertHasCorrectEncoder[Option[Short]]
    assertHasCorrectEncoder[Option[Byte]]
    assertHasCorrectEncoder[Option[Boolean]]
  }

  test("Collection types have correct schema") {
    val spark = implicitly[SparkSession]
    import spark.implicits._

    assertHasCorrectEncoder[Array[Byte]]
    assertHasCorrectEncoder[Array[Int]]
    assertHasCorrectEncoder[Array[String]]

    assertHasCorrectEncoder[Array[Option[Byte]]]
    assertHasCorrectEncoder[Array[Option[Int]]]
    assertHasCorrectEncoder[Array[Option[String]]]

    assertHasCorrectEncoder[Seq[Byte]]
    assertHasCorrectEncoder[Seq[Int]]
    assertHasCorrectEncoder[Seq[String]]

    assertHasCorrectEncoder[Seq[Option[Byte]]]
    assertHasCorrectEncoder[Seq[Option[Int]]]
    assertHasCorrectEncoder[Seq[Option[String]]]

    assertHasCorrectEncoder[Map[String, String]]
  }

  /*
  Product*  --- maybe it should be done in RecordEncoderTests
  UserDefinedType
   */

  // TODO what about the Spark types unsupported by frameless?
  //assertHasCorrectEncoder[Null]
  //assertHasCorrectEncoder[java.sql.Timestamp]
  //assertHasCorrectEncoder[java.sql.Date]
  //assertHasCorrectEncoder[java.math.BigInteger]
  //assertHasCorrectEncoder[scala.math.BigInt]
  //assertHasCorrectEncoder[org.apache.spark.sql.types.Decimal]
  //assertHasCorrectEncoder[java.lang.Integer]
  //assertHasCorrectEncoder[java.lang.Long]
  //assertHasCorrectEncoder[java.lang.Double]
  //assertHasCorrectEncoder[java.lang.Float]
  //assertHasCorrectEncoder[java.lang.Short]
  //assertHasCorrectEncoder[java.lang.Byte]
  //assertHasCorrectEncoder[java.lang.Boolean]

  //assertHasCorrectEncoder[Set[Byte]]
  //assertHasCorrectEncoder[Set[Int]]
  //assertHasCorrectEncoder[Set[String]]

  //assertHasCorrectEncoder[Set[Option[Byte]]]
  //assertHasCorrectEncoder[Set[Option[Int]]]
  //assertHasCorrectEncoder[Set[Option[String]]]
}
