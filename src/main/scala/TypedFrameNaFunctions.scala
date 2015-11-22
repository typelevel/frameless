import org.apache.spark.sql.DataFrameNaFunctions

import shapeless._
import shapeless.ops.nat.{ToInt, LT, LTEq}
import shapeless.ops.record.{Selector, SelectAll, Values}
import shapeless.ops.hlist.{ToList, Length, IsHCons, Fill}

case class TypedFrameNaFunctions[Schema](dfn: DataFrameNaFunctions) {
  def dropAny = new DropHowPartial("any")
  
  def dropAll = new DropHowPartial("all")

  class DropHowPartial(how: String) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): TypedFrame[Schema] =
        TypedFrame(columnTuple match {
          case HNil => dfn.drop(how)
          case _ => dfn.drop(how, l(columnTuple).map(_.name))
        })
  }
  
  def drop(minNonNulls: Nat) = new DropMinNNPartial(minNonNulls)
  
  class DropMinNNPartial(minNonNulls: Nat) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, NC <: Nat]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        n: Length.Aux[C, NC],
        z: LT[_0, minNonNulls.N],
        m: LTEq[minNonNulls.N, NC],
        i: ToInt[minNonNulls.N]
      ): TypedFrame[Schema] =
        TypedFrame(columnTuple match {
          case HNil => dfn.drop(i())
          case _ => dfn.drop(i(), l(columnTuple).map(_.name))
        })
  }
  
  def fillAll(value: Double): TypedFrame[Schema] = TypedFrame(dfn.fill(value))
  
  def fillAll(value: String): TypedFrame[Schema] = TypedFrame(dfn.fill(value))
  
  def fill[T](value: T) = new FillPartial[T](value)
  
  type CanFill = Int :: Long :: Float :: Double :: String :: Boolean :: HNil
  
  class FillPartial[T](value: T) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, V <: HList, F <: HList, NC <: Nat]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        s: Selector[CanFill, T],
        t: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        a: SelectAll.Aux[G, C, S],
        v: Values.Aux[S, V],
        l: Length.Aux[S, NC],
        f: Fill.Aux[NC, T, F],
        e: S =:= F
      ): TypedFrame[Schema] =
        TypedFrame(dfn.fill(t(columnTuple).map(_.name -> value).toMap))
  }
  
  type CanReplace = Double :: String :: Boolean :: HNil
  
  def replaceAll[T, G <: HList]
    (replacement: Map[T, T])
    (implicit
      r: Selector[CanReplace, T],
      g: Generic.Aux[Schema, G],
      s: Selector[G, T]
    ): TypedFrame[Schema] =
      TypedFrame(dfn.replace("*", replacement))
  
  def replace[T](replacement: Map[T, T]) = new ReplacePartial[T](replacement)
  
  class ReplacePartial[T](replacement: Map[T, T]) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, V <: HList, F <: HList, NC <: Nat]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        s: Selector[CanReplace, T],
        t: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        a: SelectAll.Aux[G, C, S],
        v: Values.Aux[S, V],
        l: Length.Aux[S, NC],
        f: Fill.Aux[NC, T, F],
        e: S =:= F
      ): TypedFrame[Schema] =
        TypedFrame(dfn.replace(t(columnTuple).map(_.name), replacement))
  }
}
