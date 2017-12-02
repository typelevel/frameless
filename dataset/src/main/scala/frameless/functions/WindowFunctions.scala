package frameless.functions

import frameless.{ OrderedWindow, TypedAggregate, TypedWindow }
import org.apache.spark.sql.{functions => untyped}

trait WindowFunctions {

  def dense_rank() = dense_rankObj

  object dense_rankObj {
    def over[T, A <: OrderedWindow](window: TypedWindow[T, A]): TypedAggregate[T, Long] = {
      new TypedAggregate[T, Long](untyped.dense_rank().over(window.untyped))
    }
  }

}
