package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{ Alias, CreateStruct }
import org.apache.spark.sql.catalyst.expressions.{ Expression, NamedExpression }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Project }
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.ObjectType
import scala.reflect.ClassTag

object FramelessInternals {

  def objectTypeFor[A](
      implicit
      classTag: ClassTag[A]
    ): ObjectType = ObjectType(classTag.runtimeClass)

  def resolveExpr(ds: Dataset[_], colNames: Seq[String]): NamedExpression = {
    ds.toDF()
      .queryExecution
      .analyzed
      .resolve(colNames, ds.sparkSession.sessionState.analyzer.resolver)
      .getOrElse {
        throw org.apache.spark.sql.ShimUtils.analysisException(ds, colNames)
      }
  }

  def expr(column: Column): Expression = column.expr

  def logicalPlan(ds: Dataset[_]): LogicalPlan = ds.logicalPlan

  def executePlan(ds: Dataset[_], plan: LogicalPlan): QueryExecution =
    ds.sparkSession.sessionState.executePlan(plan)

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
      sqlContext: SQLContext,
      plan: LogicalPlan,
      encoder: Encoder[T]
    ): Dataset[T] =
    new Dataset(sqlContext, plan, encoder)

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

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
