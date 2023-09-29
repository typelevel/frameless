package frameless

import frameless.functions.aggregate._
import frameless.functions._
import org.apache.spark.sql.types.StructType
import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.matchers.should.Matchers

class SchemaTests extends TypedDatasetSuite with Matchers {

  def structToNonNullable(struct: StructType): StructType = {
    StructType(struct.fields.map( f => f.copy(nullable = false)))
  }

  def prop[A](dataset: TypedDataset[A], ignoreNullable: Boolean = false): Prop = {
    val schema = dataset.dataset.schema

    Prop.all(
      if (!ignoreNullable)
        dataset.schema ?= schema
      else
        structToNonNullable(dataset.schema) ?= structToNonNullable(schema),
      if (!ignoreNullable)
        TypedExpressionEncoder.targetStructType(dataset.encoder) ?= schema
      else
        structToNonNullable(TypedExpressionEncoder.targetStructType(dataset.encoder))  ?= structToNonNullable(schema)
    )
  }

  test("schema of groupBy('a).agg(sum('b))") {
    val df0 = TypedDataset.create(X2(1L, 1L) :: Nil)
    val _a = df0.col('a)
    val _b = df0.col('b)

    val df = df0.groupBy(_a).agg(sum(_b))

    check(prop(df, true))
  }

  test("schema of select(lit(1L))") {
    val df0 = TypedDataset.create("test" :: Nil)
    val df = df0.select(lit(1L))

    check(prop(df))
  }

  test("schema of select(lit(1L), lit(2L)).as[X2[Long, Long]]") {
    val df0 = TypedDataset.create("test" :: Nil)
    val df = df0.select(lit(1L), lit(2L)).as[X2[Long, Long]]

    check(prop(df))
  }
}
