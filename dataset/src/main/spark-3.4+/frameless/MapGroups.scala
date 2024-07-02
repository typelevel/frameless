package frameless

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  MapGroups => SMapGroups
}

object MapGroups {

  def apply[K: Encoder, T: Encoder, U: Encoder](
      func: (K, Iterator[T]) => TraversableOnce[U],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      child: LogicalPlan
    ): LogicalPlan =
    SMapGroups(
      func,
      groupingAttributes,
      dataAttributes,
      Seq(), // #698 - no order given
      child
    )
}
