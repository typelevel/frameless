package frameless

import org.apache.spark.sql.catalyst.expressions.Expression

object FoldableImpl {
  def foldableCompat(expression: Expression): Boolean = expression.foldable
}
