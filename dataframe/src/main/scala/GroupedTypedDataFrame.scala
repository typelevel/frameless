package frameless

import shapeless._
import org.apache.spark.sql.{DataFrame, GroupedData}
import shapeless.ops.hlist.{ToList, Prepend, Length, RemoveAll, IsHCons, Mapper}
import shapeless.ops.record.{SelectAll, Values, Keys}

final class GroupedTypedDataFrame[Schema <: Product, GroupingColumns <: HList]
  (gd: GroupedData)
  (implicit val fields: Fields[Schema])
    extends Serializable {
  import GroupedTypedDataFrame._

  class DOp(theOp: Seq[String] => DataFrame) extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, N <: Nat, G <: HList, P <: HList, U <: HList, S <: HList, F <: HList, E <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        s: FieldsExtractor.Aux[Schema, C, G, U],
        a: AllRemover.Aux[G, C, P],
        i: LiftAll[Numeric, U],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        n: Length.Aux[C, N],
        f: Fill.Aux[N, Double, F],
        p: Prepend.Aux[S, F, E],
        t: XLTupler.Aux[E, Out],
        b: Fields[Out]
      ): TypedDataFrame[Out] =
        new TypedDataFrame(theOp(s(columns)))
  }

  def avg = new DOp(gd.avg)
  def mean = new DOp(gd.mean)

  object sum extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, G <: HList, P <: HList, U <: HList, S <: HList, O <: HList, E <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        s: FieldsExtractor.Aux[Schema, C, G, U],
        a: AllRemover.Aux[G, C, P],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        m: Mapper.Aux[ToPreciseNumeric.type, U, O],
        p: Prepend.Aux[S, O, E],
        t: XLTupler.Aux[E, Out],
        b: Fields[Out]
      ): TypedDataFrame[Out] =
        new TypedDataFrame(gd.sum(s(columns): _*))
  }

  class POp(theOp: Seq[String] => DataFrame) extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, G <: HList, P <: HList, U <: HList, S <: HList, E <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        s: FieldsExtractor.Aux[Schema, C, G, U],
        a: AllRemover.Aux[G, C, P],
        i: LiftAll[Numeric, U],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        p: Prepend.Aux[S, U, E],
        t: XLTupler.Aux[E, Out],
        b: Fields[Out]
      ): TypedDataFrame[Out] =
        new TypedDataFrame(theOp(s(columns)))
  }

  def max = new POp(gd.max)
  def min = new POp(gd.min)

  def count[Out <: Product, G <: HList, S <: HList, P <: HList]()
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: SelectAll.Aux[G, GroupingColumns, S],
      p: Prepend.Aux[S, Long :: HNil, P],
      t: XLTupler.Aux[P, Out],
      b: Fields[Out]
    ): TypedDataFrame[Out] =
      new TypedDataFrame(gd.count)
}

object GroupedTypedDataFrame {
  object ToPreciseNumeric extends Poly1 {
    implicit def caseByte = at[Byte](_.toLong)
    implicit def caseShort = at[Short](_.toLong)
    implicit def caseInt = at[Int](_.toLong)
    implicit def caseLong = at[Long](_.toLong)
    implicit def caseFloat = at[Float](_.toDouble)
    implicit def caseDouble = at[Double](_.toDouble)
  }
}
