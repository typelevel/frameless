package frameless.functions

import org.apache.spark.sql.Row

object DateTimeStringBehaviourUtils {

  val nullHandler: Row => Option[Int] = _.get(0) match {
    case i: Int => Some(i)
    case _      => None
  }
}
