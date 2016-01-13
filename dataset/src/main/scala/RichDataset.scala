package frameless

import shapeless._
import shapeless.ops.hlist.{IsHCons, Tupler, Selector, Length}
import shapeless.ops.record.Values

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql._

class RichDataset[Schema <: Product]
  (ds: Dataset[Schema])
  (implicit val fields: Fields[Schema])
    extends Serializable {
  
  private def df = ds.toDF().toDF(fields(): _*)
  
  /** Returns a new [[Dataset]] where each record has been mapped on to the specified type. */
  def typedAs[OtherSchema] = new TypedAsCurried[OtherSchema]
  
  protected class TypedAsCurried[OtherSchema] {
    def apply[S <: HList]()
      (implicit
        t: Generic.Aux[Schema, S],
        u: Generic.Aux[OtherSchema, S],
        e: Encoder[OtherSchema]
      ): Dataset[OtherSchema] =
        ds.as[OtherSchema]
  }
  
  /** Returns a new [[Dataset]] sorted by the specified columns, all in ascending order. */
  object sort extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor[Schema, C],
        e: Encoder[Schema]
      ): Dataset[Schema] =
        df.sort(f(columns).map(col): _*).as[Schema]
  }
  
  /** Returns a new [[Dataset]] sorted by the specified columns, all in descending order. */
  object sortDesc extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor[Schema, C],
        e: Encoder[Schema]
      ): Dataset[Schema] =
        df.sort(f(columns).map(c => col(c).desc): _*).as[Schema]
  }
  
  /** Returns a new [[Dataset]] with the selected subset of columns. */
  object selectColumns extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, S <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor.Aux[Schema, C, _, S],
        t: Tupler.Aux[S, Out],
        e: Encoder[Out]
      ): Dataset[Out] =
        df.select(f(columns).map(col): _*).as[Out]
  }
  
  /** Returns a new [[Dataset]] with columns dropped. */
  object dropColumns extends SingletonProductArgs {
    def applyProduct[C <: HList, Out <: Product, G <: HList, R <: HList, V <: HList]
      (columns: C)
      (implicit
        h: IsHCons[C],
        f: FieldsExtractor.Aux[Schema, C, G, _],
        r: AllRemover.Aux[G, C, R],
        v: Values.Aux[R, V],
        t: Tupler.Aux[V, Out],
        b: Fields[Out],
        e: Encoder[Out]
      ): Dataset[Out] = {
        val theDf = df
        f(columns).foldLeft(theDf)(_ drop theDf(_)).as[Out]
      }
  }
  
  /** Returns a new [[Dataset]] that contains only the unique rows from this [[Dataset]].
    * If a given columns arguments is provided, considers only that subset of columns. */
  object dropDuplicates extends SingletonProductArgs {
    def applyProduct[C <: HList]
      (columns: C)
      (implicit
        f: FieldsExtractor[Schema, C],
        e: Encoder[Schema]
      ): Dataset[Schema] =
        columns match {
          case HNil => df.dropDuplicates().as[Schema]
          case _ => df.dropDuplicates(f(columns)).as[Schema]
        }
  }
  
  /** Replaces values matching keys in `replacement` map with the corresponding values.
    * Key and value of `replacement` map must have the same type, either doubles or strings. */
  def replace[T](replacement: Map[T, T]) = new ReplaceCurried[T](replacement)
  
  protected class ReplaceCurried[T](replacement: Map[T, T]) extends SingletonProductArgs {
    type CanReplace = Double :: String :: HNil
    def applyProduct[C <: HList, G <: HList, S <: HList, NC <: Nat]
      (columns: C)
      (implicit
        h: IsHCons[C],
        s: Selector[CanReplace, T],
        f: FieldsExtractor.Aux[Schema, C, G, S],
        l: Length.Aux[S, NC],
        i: Fill.Aux[NC, T, S],
        e: Encoder[Schema]
      ): Dataset[Schema] =
        df.na.replace(f(columns), replacement).as[Schema]
  }
  
  // def innerJoin(otherDataset).using(columns*)
  // def outerJoin(otherDataset).using(columns*)
  // def leftOuterJoin(otherDataset).using(columns*)
  // def rightOuterJoin(otherDataset).using(columns*)
  // def leftsemiJoin(otherDataset).using(columns*)
  // def cartesianJoin(otherDataset)

  // def innerJoin(otherDataset, booleanExpr)
  // def outerJoin(otherDataset, booleanExpr)
  // def leftOuterJoin(otherDataset, booleanExpr)
  // def rightOuterJoin(otherDataset, booleanExpr)
  // def leftsemiJoin(otherDataset, booleanExpr)
  // def selectExpr(expr*)
}
