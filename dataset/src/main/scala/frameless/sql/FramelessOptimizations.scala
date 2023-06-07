package frameless.sql

import frameless.sql.rules.LiteralRule
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class FramelessOptimizations extends (SparkSessionExtensions => Unit) with Logging {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => LiteralRule)

    logInfo("Injected OptimizerRule - frameless.optimizer.LiteralRule")
  }
}
