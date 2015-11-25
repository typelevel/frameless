import org.apache.spark.sql.{DataFrameStatFunctions, DataFrame}

import shapeless._
import shapeless.nat._1
import shapeless.ops.record.{Selector, SelectAll}
import shapeless.ops.hlist.{ToList, IsHCons, Tupler}
import shapeless.tag.@@

import eu.timepit.refined.numeric.Interval.{Closed => ClosedInterval}
import eu.timepit.refined.auto._

case class TypedFrameStatFunctions[Schema](dfs: DataFrameStatFunctions) {
  def cov[G <: HList, C1, C2]
    (column1: Witness.Lt[Symbol], column2: Witness.Lt[Symbol])
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector.Aux[G, column1.T, C1],
      c: Selector.Aux[G, column2.T, C2],
      n: Numeric[C1],
      m: Numeric[C2]
    ): Double =
      dfs.cov(column1.value.name, column2.value.name)

  def corr[G <: HList, C1, C2]
    (column1: Witness.Lt[Symbol], column2: Witness.Lt[Symbol])
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector.Aux[G, column1.T, C1],
      c: Selector.Aux[G, column2.T, C2],
      n: Numeric[C1],
      m: Numeric[C2]
    ): Double =
      dfs.corr(column1.value.name, column2.value.name)
  
  // TODO?
  def crosstab[G <: HList]
    (column1: Witness.Lt[Symbol], column2: Witness.Lt[Symbol])
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector[G, column1.T],
      c: Selector[G, column2.T]
    ): DataFrame =
      dfs.crosstab(column1.value.name, column2.value.name)

  def freqItems(support: Double @@ ClosedInterval[_0, _1] = 0.01) = new FreqItemsPartial(support)
  
  class FreqItemsPartial(support: Double) extends SingletonProductArgs {
    def applyProduct[C <: HList, G <: HList, S <: HList, Out]
      (columnTuple: C)
      (implicit
        h: IsHCons[C],
        l: ToList[C, Symbol],
        g: LabelledGeneric.Aux[Schema, G],
        s: SelectAll.Aux[G, C, S],
        t: Tupler.Aux[S, Out]
      ): TypedFrame[Out] =
        TypedFrame(dfs.freqItems(l(columnTuple).map(_.name), support))
  }
  
  def sampleBy[T, G <: HList, C]
    (column: Witness.Lt[Symbol], fractions: Map[T, Double @@ ClosedInterval[_0, _1]], seed: Long)
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector.Aux[G, column.T, C],
      e: T =:= C
    ): TypedFrame[Schema] =
      TypedFrame(dfs.sampleBy(column.value.name, fractions, seed))
}
