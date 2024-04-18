package frameless

import org.apache.spark.sql.catalyst.expressions.Expression
import com.sparkutils.shim.expressions.{ And2 => And, Or2 => Or }

package object sql {

  implicit class ExpressionOps(val self: Expression) extends AnyVal {

    def toList: List[Expression] = {
      def rec(expr: Expression, acc: List[Expression]): List[Expression] = {
        expr match {
          case And(left, right) => rec(left, rec(right, acc))
          case Or(left, right)  => rec(left, rec(right, acc))
          case e                => e +: acc
        }
      }

      rec(self, Nil)
    }
  }
}
