package org.apache.spark.sql

import org.apache.spark.sql.types.DataType

object FramelessSpark32Internals {

  /**
   * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
   */
  def existsRecursively(dt: DataType)(f: (DataType) => Boolean): Boolean = dt.existsRecursively(f)
}
