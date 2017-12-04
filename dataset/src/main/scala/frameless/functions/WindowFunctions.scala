package frameless.functions

import frameless.{ OrderedWindow, TypedAggregate, TypedColumn, TypedWindow }
import org.apache.spark.sql.{ functions => untyped }

trait WindowFunctions {
import WindowFunctionsHelpers.dense_rankObj

  def dense_rank() = dense_rankObj


}

//TODO: Move these to the other funcs?
object WindowFunctions extends WindowFunctions

object WindowFunctionsHelpers {
  //TODO: How to hide this obj so that it doesn't show to users
  object dense_rankObj {
    //TODO: TypedAggregate version that can be used in `agg`
    // whose specs are all either aggs or in the groupBy. No idea how to do that later one
    def over[T, A <: OrderedWindow](window: TypedWindow[T, A]): TypedColumn[T, Int] = {
      new TypedColumn[T, Int](untyped.dense_rank().over(window.untyped))
    }
  }

}