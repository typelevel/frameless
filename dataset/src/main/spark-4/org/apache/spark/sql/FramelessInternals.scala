package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.JavaBeanEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.classic.{ColumnNodeToExpressionConverter, Dataset => ClassicDataset, ExpressionUtils, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.ObjectType
import scala.reflect.ClassTag

/**
 * Spark 4 split `Dataset`/`SparkSession`/`Column` into abstract API types
 * (`org.apache.spark.sql.*`) and concrete implementations (`org.apache.spark.sql.classic.*`).
 * The `Dataset`/`SparkSession` instances frameless holds are always the `classic`
 * implementations at runtime, so the internal-only helpers below downcast to reach the
 * `logicalPlan`/`sessionState`/`sqlContext` members that the abstract API no longer exposes.
 * `Column` no longer wraps a Catalyst `Expression`; `classic.ExpressionUtils` is Spark's
 * own bridge between the two.
 */
object FramelessInternals {

  def objectTypeFor[A](
    implicit classTag: ClassTag[A]
  ): ObjectType = ObjectType(classTag.runtimeClass)

  private def classic(ds: Dataset[_]): ClassicDataset[_] =
    ds.asInstanceOf[ClassicDataset[_]]

  def resolveExpr(ds: Dataset[_], colNames: Seq[String]): NamedExpression = {
    val cds = classic(ds)
    cds.queryExecution.analyzed
      .resolve(colNames, cds.sparkSession.sessionState.analyzer.resolver)
      .getOrElse {
        throw new AnalysisException(
          errorClass = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
          messageParameters = Map("objectName" -> colNames.mkString("."))
        )
      }
  }

  /** Wraps a Catalyst `Expression` into a `Column` (Spark 4 bridge). */
  def column(e: Expression): Column = ExpressionUtils.column(e)

  /**
   * Extracts the Catalyst `Expression` from a `Column`.
   *
   * `ExpressionUtils.expression` would return a lazy `ColumnNodeExpression` wrapper, which is
   * `Unevaluable` and exposes no children. frameless builds join plans by hand and rewrites
   * disambiguation markers via `Expression.transform`, both of which require a real, traversable
   * expression tree - so convert the column's node eagerly instead.
   */
  def expr(column: Column): Expression =
    ColumnNodeToExpressionConverter(column.node)

  def logicalPlan(ds: Dataset[_]): LogicalPlan = classic(ds).logicalPlan

  def executePlan(ds: Dataset[_], plan: LogicalPlan): QueryExecution =
    classic(ds).sparkSession.sessionState.executePlan(plan)

  def sqlContext(ds: Dataset[_]): SQLContext = classic(ds).sqlContext

  def getConf(ds: Dataset[_], key: String, default: String): String =
    classic(ds).sparkSession.conf.get(key, default)

  def joinPlan(
    ds: Dataset[_],
    plan: LogicalPlan,
    leftPlan: LogicalPlan,
    rightPlan: LogicalPlan
  ): LogicalPlan = {
    val joined = executePlan(ds, plan)
    val leftOutput = joined.analyzed.output.take(leftPlan.output.length)
    val rightOutput = joined.analyzed.output.takeRight(rightPlan.output.length)

    Project(
      List(
        Alias(CreateStruct(leftOutput), "_1")(),
        Alias(CreateStruct(rightOutput), "_2")()
      ),
      joined.analyzed
    )
  }

  def mkDataset[T](
    source: Dataset[_],
    plan: LogicalPlan,
    encoder: Encoder[T]
  ): Dataset[T] =
    new ClassicDataset[T](classic(source).sparkSession, plan, encoder)

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    ClassicDataset.ofRows(
      sparkSession.asInstanceOf[ClassicSparkSession],
      logicalPlan
    )

  /**
   * Builds an `ExpressionEncoder` from frameless' own serializer/deserializer expressions.
   *
   * Spark 4's `ExpressionEncoder` takes a leading `AgnosticEncoder` (SPARK-49025), but it is
   * only read for its `clsTag` and an Option-wrapping check - the serializer, deserializer and
   * schema are all derived from the expressions frameless supplies. A minimal `JavaBeanEncoder`
   * carrying the right `ClassTag` is therefore a correct, metadata-only stand-in.
   */
  def expressionEncoder[T](
    objSerializer: Expression,
    objDeserializer: Expression,
    classTag: ClassTag[T]
  ): ExpressionEncoder[T] =
    new ExpressionEncoder[T](
      JavaBeanEncoder(classTag, Nil),
      objSerializer,
      objDeserializer
    )

  // because org.apache.spark.sql.types.UserDefinedType is private[spark]
  type UserDefinedType[A >: Null] =
    org.apache.spark.sql.types.UserDefinedType[A]

  // below only tested in SelfJoinTests.colLeft and colRight are equivalent to col outside of joins
  //  - via files (codegen) forces doGenCode eval.
  /** Expression to tag columns from the left hand side of join expression. */
  case class DisambiguateLeft[T](tagged: Expression)
      extends Expression
      with NonSQLExpression {
    def eval(input: InternalRow): Any = tagged.eval(input)
    def nullable: Boolean = false
    def children: Seq[Expression] = tagged :: Nil
    def dataType: DataType = tagged.dataType

    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      tagged.genCode(ctx)

    protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]
    ): Expression = copy(newChildren.head)
  }

  /** Expression to tag columns from the right hand side of join expression. */
  case class DisambiguateRight[T](tagged: Expression)
      extends Expression
      with NonSQLExpression {
    def eval(input: InternalRow): Any = tagged.eval(input)
    def nullable: Boolean = false
    def children: Seq[Expression] = tagged :: Nil
    def dataType: DataType = tagged.dataType

    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      tagged.genCode(ctx)

    protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]
    ): Expression = copy(newChildren.head)
  }
}
