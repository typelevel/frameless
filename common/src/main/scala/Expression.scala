package frameless

import org.apache.spark.sql.Column
import scala.math.Ordering
import shapeless.Witness

final class DataColumn[K, A] private[frameless](val column: Column) {
  import DataColumn._

  def !==(other: A): TempDataColumn[Boolean] = DataColumn(column !== other)
  def !==(other: DataColumn[_, A]): TempDataColumn[Boolean] = DataColumn(column !== other)

  def %(other: A)(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column % other)
  def %(other: DataColumn[_, A])(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column % other)

  def &&(other: Boolean)(implicit ev: A =:= Boolean): TempDataColumn[Boolean] = DataColumn(column && other)
  def &&(other: DataColumn[_, Boolean])(implicit ev: A =:= Boolean): TempDataColumn[Boolean] = DataColumn(column && other)

  def *(other: A)(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column * other)
  def *(other: DataColumn[_, A])(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column * other)

  def +(other: A)(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column + other)
  def +(other: DataColumn[_, A])(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column + other)

  def -(other: A)(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column - other)
  def -(other: DataColumn[_, A])(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column - other)

  // Need to look into this
  def /(other: A)(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column / other)
  def /(other: DataColumn[_, A])(implicit A: Numeric[A]): TempDataColumn[A] = DataColumn(column / other)

  def <(other: A)(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column < other)
  def <(other: DataColumn[_, A])(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column < other)

  def <=(other: A)(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column <= other)
  def <=(other: DataColumn[_, A])(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column <= other)

  def <=>(other: A): TempDataColumn[Boolean] = DataColumn(column <=> other)
  def <=>(other: DataColumn[_, A]): TempDataColumn[Boolean] = DataColumn(column <=> other)

  def ===(other: A): TempDataColumn[Boolean] = DataColumn(column === other)
  def ===(other: DataColumn[_, A]): TempDataColumn[Boolean] = DataColumn(column === other)

  def >(other: A)(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column > other)
  def >(other: DataColumn[_, A])(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column > other)

  def >=(other: A)(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column >= other)
  def >=(other: DataColumn[_, A])(implicit A: Ordering[A]): TempDataColumn[Boolean] = DataColumn(column >= other)

  def as(alias: Witness.Lt[Symbol]): DataColumn[alias.T, A] = DataColumn(column.as(alias.value.name))

  def asc(implicit A: Ordering[A]): TempDataColumn[A] = DataColumn(column.asc)

  def cast[B >: A]: TempDataColumn[B] = DataColumn(column)

  // def cast[B <% A]: TempDataColumn[B] = DataColumn(column) // Deprecated

  def contains(other: A): TempDataColumn[Boolean] = DataColumn(column.contains(other))

  def desc(implicit A: Ordering[A]): TempDataColumn[A] = DataColumn(column.desc)

  def endsWith(literal: String)(implicit ev: A =:= String): TempDataColumn[Boolean] =
    DataColumn(column.endsWith(literal))

  def endsWith(other: DataColumn[_, A])(implicit ev: A =:= String): TempDataColumn[Boolean] =
    DataColumn(column.endsWith(other.column))

  def equalTo(other: A): TempDataColumn[Boolean] = DataColumn(column.equalTo(other))
  def equalTo(other: DataColumn[_, A]): TempDataColumn[Boolean] = DataColumn(column.equalTo(other))

  def explain(extended: Boolean): Unit = column.explain(extended)

  def in(list: DataColumn[_, A]*): TempDataColumn[Boolean] = DataColumn(column.isin(list.map(_.column): _*))

  def isNotNull: TempDataColumn[Boolean] = DataColumn(column.isNotNull)

  def isNull: TempDataColumn[Boolean] = DataColumn(column.isNull)

  def startsWith(literal: String)(implicit ev: A =:= String): TempDataColumn[Boolean] =
    DataColumn(column.startsWith(literal))

  def startsWith(other: DataColumn[_, A])(implicit ev: A =:= String): TempDataColumn[Boolean] =
    DataColumn(column.startsWith(other.column))

  def substr(startPos: Int, len: Int)(implicit ev: A =:= String): TempDataColumn[String] =
    DataColumn(column.substr(startPos, len))

  def substr(startPos: DataColumn[_, Int], len: DataColumn[_, Int])(implicit ev: A =:= String): TempDataColumn[String] =
    DataColumn(column.substr(startPos.column, len.column))

  override def toString(): String = s"DataColumn:\n${column.toString}"

  def unary_!(implicit ev: A =:= Boolean): TempDataColumn[Boolean] = DataColumn(!column)

  def unary_-(implicit ev: Numeric[A]): TempDataColumn[A] = DataColumn(-column)

  def ||(other: Boolean)(implicit ev: A =:= Boolean): TempDataColumn[Boolean] = DataColumn(column || other)
  def ||(other: DataColumn[_, Boolean])(implicit ev: A =:= Boolean): TempDataColumn[Boolean] = DataColumn(column || other)
}

object DataColumn {
  val TempDataColumn = Witness('TempColumn)
  type TempDataColumn[A] = DataColumn[TempDataColumn.T, A]

  def apply[K, A](column: Column): DataColumn[K, A] = new DataColumn(column)
}
