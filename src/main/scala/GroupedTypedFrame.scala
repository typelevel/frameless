package typedframe

import shapeless._
import org.apache.spark.sql.{DataFrame, GroupedData}
import shapeless.ops.hlist.{ToList, Prepend, Length, Fill, RemoveAll, IsHCons, Mapper}
import shapeless.ops.record.{SelectAll, Values, Keys}

final class GroupedTypedFrame[Schema <: Product, GroupingColumns <: HList](gd: GroupedData) extends Serializable {
  import GroupedTypedFrame._
  
  class DOp(theOp: Seq[String] => DataFrame) extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, N <: Nat, G <: HList, P <: HList, U <: HList, S <: HList, F <: HList, E <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        h: IsHCons[C],
        g: LabelledGeneric.Aux[Schema, G],
        a: AllRemover.Aux[G, C, P],
        s: SelectAll.Aux[G, C, U],
        i: LiftAll[Numeric, U],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        n: Length.Aux[C, N],
        f: Fill.Aux[N, Double, F],
        p: Prepend.Aux[S, F, E],
        t: XLTupler.Aux[E, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(theOp(columnTuple.toList.map(_.name)))
  }
  
  def avg = new DOp(gd.avg)
  def mean = new DOp(gd.mean)
  
  object sum extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, G <: HList, P <: HList, U <: HList, S <: HList, O <: HList, E <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        h: IsHCons[C],
        g: LabelledGeneric.Aux[Schema, G],
        a: AllRemover.Aux[G, C, P],
        s: SelectAll.Aux[G, C, U],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        m: Mapper.Aux[ToPreciseNumeric.type, U, O],
        p: Prepend.Aux[S, O, E],
        t: XLTupler.Aux[E, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(gd.sum(columnTuple.toList.map(_.name): _*))
  }
  
  class POp(theOp: Seq[String] => DataFrame) extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, N <: Nat, G <: HList, P <: HList, U <: HList, S <: HList, E <: HList, B <: HList, Y <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        h: IsHCons[C],
        g: LabelledGeneric.Aux[Schema, G],
        a: AllRemover.Aux[G, C, P],
        s: SelectAll.Aux[G, C, U],
        i: LiftAll[Numeric, U],
        e: SelectAll[P, GroupingColumns],
        r: SelectAll.Aux[G, GroupingColumns, S],
        n: Length.Aux[C, N],
        p: Prepend.Aux[S, U, E],
        t: XLTupler.Aux[E, Out],
        b: LabelledGeneric.Aux[Out, B],
        y: Keys.Aux[B, Y],
        o: ToList[Y, Symbol]
      ): TypedFrame[Out] =
        TypedFrame(theOp(columnTuple.toList.map(_.name)))
  }
  
  def max = new POp(gd.max)
  def min = new POp(gd.min)
  
  def count[Out <: Product, G <: HList, S <: HList, P <: HList, B <: HList, Y <: HList]()
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: SelectAll.Aux[G, GroupingColumns, S],
      p: Prepend.Aux[S, Long :: HNil, P],
      t: XLTupler.Aux[P, Out],
      b: LabelledGeneric.Aux[Out, B],
      y: Keys.Aux[B, Y],
      o: ToList[Y, Symbol]
    ): TypedFrame[Out] =
      TypedFrame(gd.count)
}

object GroupedTypedFrame {
  object ToPreciseNumeric extends Poly1 {
    implicit def caseByte = at[Byte](_.toLong)
    implicit def caseShort = at[Short](_.toLong)
    implicit def caseInt = at[Int](_.toLong)
    implicit def caseLong = at[Long](_.toLong)
    implicit def caseFloat = at[Float](_.toDouble)
    implicit def caseDouble = at[Double](_.toDouble)
  }
}
