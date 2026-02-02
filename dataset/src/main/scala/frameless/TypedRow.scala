package frameless

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ DataType, ObjectType }
import shapeless.HList

case class TypedRow[T <: HList](row: Row) {

  def apply(i: Int): Any = row.apply(i)
}

object TypedRow {

  def apply(values: Any*): TypedRow[HList] = {

    val row = Row.fromSeq(values)
    TypedRow(row)
  }

  case class WithCatalystTypes(schema: Seq[DataType]) {

    def fromInternalRow(row: InternalRow): TypedRow[HList] = {
      val data = row.toSeq(schema).toArray

      apply(data: _*)
    }

  }

  object WithCatalystTypes {}

  def fromHList[T <: HList](
      hlist: T
    ): TypedRow[T] = {

    val cells = hlist.runtimeList

    val row = Row.fromSeq(cells)
    TypedRow(row)
  }

  lazy val catalystType: ObjectType = ObjectType(classOf[TypedRow[_]])

}
