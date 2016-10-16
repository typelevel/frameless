package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.ObjectType

import scala.reflect.ClassTag

object FramelessInternals {
  def objectTypeFor[A](implicit classTag: ClassTag[A]): ObjectType = ObjectType(classTag.runtimeClass)

  def resolveExpr(ds: Dataset[_], colNames: Seq[String]): NamedExpression = {
    ds.toDF.queryExecution.analyzed.resolve(colNames, ds.sparkSession.sessionState.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colNames" among (${ds.schema.fieldNames.mkString(", ")})""")
    }
  }

  def expr(column: Column): Expression = column.expr

  def column(column: Column): Expression = column.expr

  def logicalPlan(ds: Dataset[_]): LogicalPlan = ds.logicalPlan

  def executePlan(ds: Dataset[_], plan: LogicalPlan): QueryExecution = {
    ds.sparkSession.sessionState.executePlan(plan)
  }

  def mkDataset[T](sqlContext: SQLContext, plan: LogicalPlan, encoder: Encoder[T]): Dataset[T] = {
    new Dataset(sqlContext, plan, encoder)
  }
}
