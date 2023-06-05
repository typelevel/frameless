package frameless.optimiser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class FramelessExtension extends ((SparkSessionExtensions) => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule( _ => LiteralRule)
  }
}
