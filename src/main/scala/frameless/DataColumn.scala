package frameless

import org.apache.spark.sql.Column

import scala.math.Ordering

final class DataColumn[A] private[frameless](val column: Column) {
  def !==(other: A): DataColumn[Boolean] = DataColumn(column !== other)
  def !==(other: DataColumn[A]): DataColumn[Boolean] = DataColumn(column !== other)

  def %(other: A)(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column % other)
  def %(other: DataColumn[A])(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column % other)

  def &&(other: Boolean)(implicit ev: A =:= Boolean): DataColumn[Boolean] = DataColumn(column && other)
  def &&(other: DataColumn[Boolean])(implicit ev: A =:= Boolean): DataColumn[Boolean] = DataColumn(column && other)

  def *(other: A)(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column * other)
  def *(other: DataColumn[A])(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column * other)

  def +(other: A)(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column + other)
  def +(other: DataColumn[A])(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column + other)

  def -(other: A)(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column - other)
  def -(other: DataColumn[A])(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column - other)

  // Need to look into this
  def /(other: A)(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column / other)
  def /(other: DataColumn[A])(implicit A: Numeric[A]): DataColumn[A] = DataColumn(column / other)

  def <(other: A)(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column < other)
  def <(other: DataColumn[A])(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column < other)

  def <=(other: A)(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column <= other)
  def <=(other: DataColumn[A])(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column <= other)

  def <=>(other: A): DataColumn[Boolean] = DataColumn(column <=> other)
  def <=>(other: DataColumn[A]): DataColumn[Boolean] = DataColumn(column <=> other)

  def ===(other: A): DataColumn[Boolean] = DataColumn(column === other)
  def ===(other: DataColumn[A]): DataColumn[Boolean] = DataColumn(column === other)

  def >(other: A)(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column > other)
  def >(other: DataColumn[A])(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column > other)

  def >=(other: A)(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column >= other)
  def >=(other: DataColumn[A])(implicit A: Ordering[A]): DataColumn[Boolean] = DataColumn(column >= other)

  def as(alias: Symbol): Column = ???

  def as(alias: String): Column = ???

  def asc(implicit A: Ordering[A]): DataColumn[A] = DataColumn(column.asc)

  def cast[B >: A]: DataColumn[B] = DataColumn(column)

  def cast[B <% A]: DataColumn[B] = DataColumn(column)

  def contains(other: Any): Column = ???

  def desc(implicit A: Ordering[A]): DataColumn[A] = DataColumn(column.desc)

  def endsWith(literal: String)(implicit ev: A =:= String): DataColumn[Boolean] =
    DataColumn(column.endsWith(literal))

  def endsWith(other: DataColumn[A])(implicit ev: A =:= String): DataColumn[Boolean] =
    DataColumn(column.endsWith(other.column))

  def equalTo(other: A): DataColumn[Boolean] = DataColumn(column.equalTo(other))
  def equalTo(other: DataColumn[A]): DataColumn[Boolean] = DataColumn(column.equalTo(other))

  def explain(extended: Boolean): Unit = column.explain(extended)

  def getField(fieldName: String): Column = ???

  def getItem(ordinal: Int): Column = ???

  def in(list: DataColumn[A]*): DataColumn[Boolean] = DataColumn(column.in(list.map(_.column): _*))

  def isNotNull: DataColumn[Boolean] = DataColumn(column.isNotNull)

  def isNull: DataColumn[Boolean] = DataColumn(column.isNull)

  def like(literal: String): Column = ???

  def rlike(literal: String): Column = ???

  def startsWith(literal: String)(implicit ev: A =:= String): DataColumn[Boolean] =
    DataColumn(column.startsWith(literal))

  def startsWith(other: DataColumn[A])(implicit ev: A =:= String): DataColumn[Boolean] =
    DataColumn(column.startsWith(other.column))

  def substr(startPos: Int, len: Int)(implicit ev: A =:= String): DataColumn[String] =
    DataColumn(column.substr(startPos, len))

  def substr(startPos: DataColumn[Int], len: DataColumn[Int])(implicit ev: A =:= String): DataColumn[String] =
    DataColumn(column.substr(startPos.column, len.column))

  override def toString(): String = s"DataColumn:\n${column.toString}"

  def unary_!(implicit ev: A =:= Boolean): DataColumn[Boolean] = DataColumn(!column)

  def unary_-(implicit ev: Numeric[A]): DataColumn[A] = DataColumn(-column)

  def ||(other: Boolean)(implicit ev: A =:= Boolean): DataColumn[Boolean] = DataColumn(column || other)
  def ||(other: DataColumn[Boolean])(implicit ev: A =:= Boolean): DataColumn[Boolean] = DataColumn(column || other)
}

object DataColumn {
  def apply[A](column: Column): DataColumn[A] = new DataColumn(column)
}
