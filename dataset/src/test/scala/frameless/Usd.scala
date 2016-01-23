package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, StaticInvoke, Invoke}
import org.apache.spark.sql.types._

import org.scalacheck._

case class Usd(value: BigDecimal)

object Usd {
  implicit val isomorphism: Isomorphism[Usd, BigDecimal] = Isomorphism(_.value, Usd(_))

  implicit val arbitrary: Arbitrary[Usd] = Arbitrary {
    Arbitrary.arbitrary[BigDecimal].map(Usd(_))
  }

  implicit val encoder: TypedEncoder[Usd] = new PrimitiveTypedEncoder[Usd] {
    private val decimalEncoder = TypedEncoder[BigDecimal]

    def nullable: Boolean = false

    def sourceDataType: DataType = ScalaReflection.dataTypeFor[Usd]
    def targetDataType: DataType = decimalEncoder.targetDataType

    def constructorFor(path: Expression): Expression = {
      StaticInvoke(
        staticObject = Usd.getClass,
        dataType = sourceDataType,
        functionName = "apply",
        arguments = decimalEncoder.constructorFor(path) :: Nil,
        propagateNull = true
      )
    }

    def extractorFor(path: Expression): Expression = {
      decimalEncoder.extractorFor(Invoke(path, "value", decimalEncoder.sourceDataType, Nil))
    }
  }
}
