package typedframe

import shapeless._
import shapeless.ops.hlist.Length
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.Row

trait TypeableRow[S <: Product] extends Serializable {
  def apply(row: Row): S
}

trait LowPriorityTypeableRow {
  implicit def typeableRowProduct[S <: Product, G <: HList]
    (implicit
      g: Generic.Aux[S, G],
      c: TypeTag[G],
      n: FromTraversableNullable[G]
    ): TypeableRow[S] =
      new TypeableRow[S] {
        def apply(row: Row): S = n(row.toSeq).fold(fail(row))(g.from)
      }
  
  protected def fail[G](row: Row)(implicit c: TypeTag[G]) =
    throw new RuntimeException(s"Type error: failed to cast row $row of type ${row.schema} to $c")
}

object TypeableRow extends LowPriorityTypeableRow {
  def apply[S <: Product](implicit t: TypeableRow[S]): TypeableRow[S] = t
  
  implicit def typeableRowTuple[S <: Product, G <: HList, N <: Nat, F <: HList, T <: Product]
    (implicit
      t: IsXLTuple[S],
      g: Generic.Aux[S, G],
      c: TypeTag[G],
      l: Length.Aux[G, N],
      f: Fille.Aux[N, Any, F],
      n: FromTraversableNullable[F],
      p: XLTupler.Aux[F, T]
    ): TypeableRow[S] =
      new TypeableRow[S] {
        def apply(row: Row): S = n(row.toSeq).fold(fail(row))(l => p(l).asInstanceOf[S])
      }
}
