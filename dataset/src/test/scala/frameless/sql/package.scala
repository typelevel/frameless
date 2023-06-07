package frameless

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.expressions.{And, Or}

package object sql {
  implicit class TypedDatasetOps[T](val self: TypedDataset[T]) {
    def pushDownFilters: List[Filter] = {
      val sparkPlan = self.queryExecution.executedPlan

      val initialPlan =
        if (sparkPlan.children.isEmpty) // assume it's AQE
          sparkPlan match {
            case aq: AdaptiveSparkPlanExec => aq.initialPlan
            case _ => sparkPlan
          }
        else
          sparkPlan

      initialPlan.collect {
        case fs: FileSourceScanExec =>
          import scala.reflect.runtime.{universe => ru}

          val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
          val instanceMirror = runtimeMirror.reflect(fs)
          val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
          val m = instanceMirror.reflectMethod(getter.asMethod)
          val res = m.apply(fs).asInstanceOf[Seq[Filter]]

          res
      }.flatten.toList
    }
  }

  implicit class ExpressionOps(val self: Expression) extends AnyVal {
    def toList: List[Expression] = {
      def rec(expr: Expression, acc: List[Expression]): List[Expression] = {
        expr match {
          case And(left, right) => rec(left, rec(right, acc))
          case Or(left, right) => rec(left, rec(right, acc))
          case e => e +: acc
        }
      }

      rec(self, Nil)
    }
  }
}
