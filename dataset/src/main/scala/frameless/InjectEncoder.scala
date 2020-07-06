package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

case class InjectEncoder[A: ClassTag, B]()
  (implicit inj: Injection[A, B], trb: TypedEncoder[B]) extends TypedEncoder[A] {
  def nullable: Boolean = trb.nullable
  def jvmRepr: DataType = FramelessInternals.objectTypeFor[A](classTag)
  def catalystRepr: DataType = trb.catalystRepr

  def fromCatalyst(path: Expression): Expression = {
    val bexpr = trb.fromCatalyst(path)
    Invoke(Literal.fromObject(inj), "invert", jvmRepr, Seq(bexpr))
  }

  def toCatalyst(path: Expression): Expression = {
    val invoke = Invoke(Literal.fromObject(inj), "apply", trb.jvmRepr, Seq(path))
    trb.toCatalyst(invoke)
  }
}
