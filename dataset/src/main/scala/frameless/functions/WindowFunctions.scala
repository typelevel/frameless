package frameless.functions

import frameless.{ OrderedWindow, TypedColumn, TypedWindow }
import org.apache.spark.sql.{ functions => untyped }

trait WindowFunctions {

  //TODO: TypedAggregate version that can be used in `agg`
  // whose specs are all either aggs or in the groupBy. Not sure how to do the latter one
  def denseRank[T, A <: OrderedWindow](over: TypedWindow[T, A]): TypedColumn[T, Int] = {
    new TypedColumn[T, Int](untyped.dense_rank().over(over.untyped))
  }

}

//TODO: Move these to the other funcs?
object WindowFunctions extends WindowFunctions
