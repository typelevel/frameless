package frameless.ml.params.linears

/**
  * solver algorithm used for optimization.
  *  - "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton
  *    optimization method.
  *  - "normal" denotes using Normal Equation as an analytical solution to the linear regression
  *    problem.  This solver is limited to `LinearRegression.MAX_FEATURES_FOR_NORMAL_SOLVER`.
  *  - "auto" (default) means that the solver algorithm is selected automatically.
  *    The Normal Equations solver will be used when possible, but this will automatically fall
  *    back to iterative optimization methods when needed.
  *
  *    spark
  */

sealed abstract class Solver private[ml](val sparkValue: String)
object Solver {
  case object LBFGS   extends Solver("l-bfgs")
  case object Auto    extends Solver("auto")
  case object Normal  extends Solver("normal")
}

