package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.ScalaReflection.{cleanUpReflectionObjects, getClassFromType, getClassNameFromType, localTypeOf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.unsafe.types.CalendarInterval

import scala.reflect.ClassTag

private[sql] object ScalaSubtypeLock

object FramelessInternals {

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  // Since we are creating a runtime mirror using the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  def mirror: universe.Mirror = {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  import universe._

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Special handling is also used for Arrays including
   * those that hold primitive types.
   *
   * Unlike `schemaFor`, this function doesn't do any massaging of types into the Spark SQL type
   * system.  As a result, ObjectType will be returned for things like boxed Integers
   */
  def dataTypeFor[T : TypeTag]: DataType = dataTypeFor(localTypeOf[T])

  /**
   * Synchronize to prevent concurrent usage of `<:<` operator.
   * This operator is not thread safe in any current version of scala; i.e.
   * (2.11.12, 2.12.10, 2.13.0-M5).
   *
   * See https://github.com/scala/bug/issues/10766
   */
  private[sql] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  private def dataTypeFor(tpe: `Type`): DataType = cleanUpReflectionObjects {
    tpe.dealias match {
      case t if isSubtype(t, definitions.NullTpe) => NullType
      case t if isSubtype(t, definitions.IntTpe) => IntegerType
      case t if isSubtype(t, definitions.LongTpe) => LongType
      case t if isSubtype(t, definitions.DoubleTpe) => DoubleType
      case t if isSubtype(t, definitions.FloatTpe) => FloatType
      case t if isSubtype(t, definitions.ShortTpe) => ShortType
      case t if isSubtype(t, definitions.ByteTpe) => ByteType
      case t if isSubtype(t, definitions.BooleanTpe) => BooleanType
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => BinaryType
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => CalendarIntervalType
      case t if isSubtype(t, localTypeOf[Decimal]) => DecimalType.SYSTEM_DEFAULT
      case _ =>
        val className = getClassNameFromType(tpe)
        className match {
          case "scala.Array" =>
            val TypeRef(_, _, Seq(elementType)) = tpe.dealias
            arrayClassFor(elementType)
          case other =>
            val clazz = getClassFromType(tpe)
            ObjectType(clazz)
        }
    }
  }

  /**
   * Given a type `T` this function constructs `ObjectType` that holds a class of type
   * `Array[T]`.
   *
   * Special handling is performed for primitive types to map them back to their raw
   * JVM form instead of the Scala Array that handles auto boxing.
   */
  private def arrayClassFor(tpe: `Type`): ObjectType = cleanUpReflectionObjects {
    val cls = tpe.dealias match {
      case t if isSubtype(t, definitions.IntTpe) => classOf[Array[Int]]
      case t if isSubtype(t, definitions.LongTpe) => classOf[Array[Long]]
      case t if isSubtype(t, definitions.DoubleTpe) => classOf[Array[Double]]
      case t if isSubtype(t, definitions.FloatTpe) => classOf[Array[Float]]
      case t if isSubtype(t, definitions.ShortTpe) => classOf[Array[Short]]
      case t if isSubtype(t, definitions.ByteTpe) => classOf[Array[Byte]]
      case t if isSubtype(t, definitions.BooleanTpe) => classOf[Array[Boolean]]
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => classOf[Array[Array[Byte]]]
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => classOf[Array[CalendarInterval]]
      case t if isSubtype(t, localTypeOf[Decimal]) => classOf[Array[Decimal]]
      case other =>
        // There is probably a better way to do this, but I couldn't find it...
        val elementType = dataTypeFor(other).asInstanceOf[ObjectType].cls
        java.lang.reflect.Array.newInstance(elementType, 0).getClass

    }
    ObjectType(cls)
  }

  /**
   * Returns true if the value of this data type is same between internal and external.
   */
  def isNativeType(dt: DataType): Boolean = dt match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType | CalendarIntervalType => true
    case _ => false
  }

  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }


  def objectTypeFor[A](implicit classTag: ClassTag[A]): ObjectType = ObjectType(classTag.runtimeClass)

  def resolveExpr(ds: Dataset[_], colNames: Seq[String]): NamedExpression = {
    ds.toDF().queryExecution.analyzed.resolve(colNames, ds.sparkSession.sessionState.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colNames" among (${ds.schema.fieldNames.mkString(", ")})""")
    }
  }

  def expr(column: Column): Expression = column.expr

  def column(column: Column): Expression = column.expr

  def logicalPlan(ds: Dataset[_]): LogicalPlan = ds.logicalPlan

  def executePlan(ds: Dataset[_], plan: LogicalPlan): QueryExecution =
    ds.sparkSession.sessionState.executePlan(plan)

  def joinPlan(ds: Dataset[_], plan: LogicalPlan, leftPlan: LogicalPlan, rightPlan: LogicalPlan): LogicalPlan = {
    val joined = executePlan(ds, plan)
    val leftOutput = joined.analyzed.output.take(leftPlan.output.length)
    val rightOutput = joined.analyzed.output.takeRight(rightPlan.output.length)

    Project(List(
      Alias(CreateStruct(leftOutput), "_1")(),
      Alias(CreateStruct(rightOutput), "_2")()
    ), joined.analyzed)
  }

  def mkDataset[T](sqlContext: SQLContext, plan: LogicalPlan, encoder: Encoder[T]): Dataset[T] =
    new Dataset(sqlContext, plan, encoder)

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

  // because org.apache.spark.sql.types.UserDefinedType is private[spark]
  type UserDefinedType[A >: Null] =  org.apache.spark.sql.types.UserDefinedType[A]

  /** Expression to tag columns from the left hand side of join expression. */
  case class DisambiguateLeft[T](tagged: Expression) extends Expression with NonSQLExpression {
    def eval(input: InternalRow): Any = tagged.eval(input)
    def nullable: Boolean = false
    def children: Seq[Expression] = tagged :: Nil
    def dataType: DataType = tagged.dataType
    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = tagged.genCode(ctx)
    protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)
  }

  /** Expression to tag columns from the right hand side of join expression. */
  case class DisambiguateRight[T](tagged: Expression) extends Expression with NonSQLExpression {
    def eval(input: InternalRow): Any = tagged.eval(input)
    def nullable: Boolean = false
    def children: Seq[Expression] = tagged :: Nil
    def dataType: DataType = tagged.dataType
    protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = tagged.genCode(ctx)
    protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)
  }
}
