package frameless

import org.apache.spark.sql.{DataFrameStatFunctions, DataFrame}

import shapeless._
import shapeless.nat._1
import shapeless.ops.record.{Selector, Keys}
import shapeless.ops.hlist.{ToList, IsHCons, Tupler}

final class TypedFrameStatFunctions[Schema <: Product] private[frameless]
  (dfs: DataFrameStatFunctions)
  (implicit val fields: Fields[Schema])
    extends Serializable {
  
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
  
  // This can't a TypedFrame since the output columns depend on the dataframe
  // content: there is one column for each different value accross all rows.
  def crosstab[G <: HList]
    (column1: Witness.Lt[Symbol], column2: Witness.Lt[Symbol])
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector[G, column1.T],
      c: Selector[G, column2.T]
    ): DataFrame =
      dfs.crosstab(column1.value.name, column2.value.name)
  
  def freqItems(support: Double = 0.01) = new FreqItemsCurried(support)
  
  class FreqItemsCurried(support: Double) extends SingletonProductArgs {
    def applyProduct[Out <: Product, C <: HList, S <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor.Aux[Schema, C, _, S],
        t: Tupler.Aux[S, Out],
        b: Fields[Out]
      ): TypedFrame[Out] =
        new TypedFrame(dfs.freqItems(f(columns), support))
  }
  
  def sampleBy[T, G <: HList, C]
    (column: Witness.Lt[Symbol], fractions: Map[T, Double], seed: Long)
    (implicit
      g: LabelledGeneric.Aux[Schema, G],
      s: Selector.Aux[G, column.T, C],
      e: T =:= C
    ): TypedFrame[Schema] =
      new TypedFrame(dfs.sampleBy(column.value.name, fractions, seed))
}
