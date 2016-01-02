package typedframe

import org.apache.spark.sql.DataFrameNaFunctions

import shapeless._
import shapeless.ops.nat.{ToInt, LT, LTEq}
import shapeless.ops.record.{SelectAll, Values}
import shapeless.ops.hlist.{ToList, Length, IsHCons, Fill, Selector}
import shapeless.tag.@@

import eu.timepit.refined.numeric.Positive

final class TypedFrameNaFunctions[Schema <: Product] private[typedframe]
  (dfn: DataFrameNaFunctions)
  (implicit val fields: Fields[Schema])
    extends Serializable {
  
  def dropAny = new DropHowCurried("any")
  
  def dropAll = new DropHowCurried("all")
  
  class DropHowCurried(how: String) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList]
      (columnTuple: C)
      (implicit
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        r: SelectAll[G, C]
      ): TypedFrame[Schema] =
        new TypedFrame(columnTuple match {
          case HNil => dfn.drop(how)
          case _ => dfn.drop(how, l(columnTuple).map(_.name))
        })
  }
  
  def drop(minNonNulls: Int @@ Positive) = new DropMinNNCurried(minNonNulls)
  
  class DropMinNNCurried(minNonNulls: Int @@ Positive) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S]
      ): TypedFrame[Schema] =
        new TypedFrame(dfn.drop(minNonNulls, l(columnTuple).map(_.name)))
  }
  
  def fillAll(value: Double): TypedFrame[Schema] = new TypedFrame(dfn.fill(value))
  
  def fillAll(value: String): TypedFrame[Schema] = new TypedFrame(dfn.fill(value))
  
  def fill[T](value: T) = new FillCurried[T](value)
  
  type CanFill = Int :: Long :: Float :: Double :: String :: HNil
  
  class FillCurried[T](value: T) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, F <: HList, NC <: Nat]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        s: Selector[CanFill, T],
        t: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        a: SelectAll.Aux[G, C, S],
        l: Length.Aux[S, NC],
        f: Fill.Aux[NC, T, F],
        e: S =:= F
      ): TypedFrame[Schema] =
        new TypedFrame(dfn.fill(t(columnTuple).map(_.name -> value).toMap))
  }
  
  type CanReplace = Double :: String :: HNil
  
  def replaceAll[T, G <: HList]
    (replacement: Map[T, T])
    (implicit
      r: Selector[CanReplace, T],
      g: Generic.Aux[Schema, G],
      s: Selector[G, T]
    ): TypedFrame[Schema] =
      new TypedFrame(dfn.replace("*", replacement))
  
  def replace[T](replacement: Map[T, T]) = new ReplaceCurried[T](replacement)
  
  class ReplaceCurried[T](replacement: Map[T, T]) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, F <: HList, NC <: Nat]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        s: Selector[CanReplace, T],
        t: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        a: SelectAll.Aux[G, C, S],
        l: Length.Aux[S, NC],
        f: Fill.Aux[NC, T, F],
        e: S =:= F
      ): TypedFrame[Schema] =
        new TypedFrame(dfn.replace(t(columnTuple).map(_.name), replacement))
  }
  
}
