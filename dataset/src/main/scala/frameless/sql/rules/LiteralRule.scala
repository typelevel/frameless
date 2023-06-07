package frameless.sql.rules

//import frameless.functions.Lit
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object LiteralRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan/*.transformExpressions {
    // replace all literals to allow constant folding and push down
    case Lit(dataType, _, _, convertedValue) => Literal(convertedValue.eval(), dataType)
  }*/

  def registerOptimizations(sqlContext: SQLContext): Unit = ()/*
    Seq(LiteralRule).foreach { r =>
      if (!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }*/
}
