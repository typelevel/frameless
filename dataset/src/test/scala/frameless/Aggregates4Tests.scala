package frameless

case class Averager4Tests[A: Numeric, B: Numeric](avg: Seq[A] => B)
object Averager4Tests {
  // Replicate Spark's behaviour : If the datatype isn't BigDecimal cast type to Double
  // https://github.com/apache/spark/blob/7eb2ca8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L50
  implicit def averageDecimal = Averager4Tests[BigDecimal, BigDecimal](as => as.sum/as.size)
  implicit def averageDouble = Averager4Tests[Double, Double](as => as.sum/as.size)
  implicit def averageLong = Averager4Tests[Long, Double](as => as.map(_.toDouble).sum/as.size)
  implicit def averageInt = Averager4Tests[Int, Double](as => as.map(_.toDouble).sum/as.size)
  implicit def averageShort = Averager4Tests[Short, Double](as => as.map(_.toDouble).sum/as.size)
}

case class Sum4Tests[A, B](sum: Seq[A] => B)
object Sum4Tests {
  // Replicate Spark's behaviour : Ints and Shorts are cast to Long
  // https://github.com/apache/spark/blob/7eb2ca8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L37
  implicit def summerDecimal = Sum4Tests[BigDecimal, BigDecimal](_.sum)
  implicit def summerDouble = Sum4Tests[Double, Double](_.sum)
  implicit def summerLong = Sum4Tests[Long, Long](_.sum)
  implicit def summerInt = Sum4Tests[Int, Long](_.map(_.toLong).sum)
  implicit def summerShort = Sum4Tests[Short, Long](_.map(_.toLong).sum)
}

