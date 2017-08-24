package frameless

import org.apache.spark.sql.FramelessInternals

package object udt {
  type Udt[A >: Null] = FramelessInternals.PublicUserDefinedType[A]
}
