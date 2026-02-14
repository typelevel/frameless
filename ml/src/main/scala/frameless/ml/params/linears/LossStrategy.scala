package frameless
package ml
package params
package linears

/**
 * <a href="https://en.wikipedia.org/wiki/Mean_squared_error">SquaredError</a>  measures the average of the squares of the errors—that is,
 * the average squared difference between the estimated values and what is estimated.
 *
 * <a href="https://en.wikipedia.org/wiki/Huber_loss">Huber Loss</a>  loss function less sensitive to outliers in data than the
 * squared error loss
 */
sealed abstract class LossStrategy private[ml] (val sparkValue: String)

object LossStrategy {
  case object SquaredError extends LossStrategy("squaredError")
  case object Huber extends LossStrategy("huber")
}
