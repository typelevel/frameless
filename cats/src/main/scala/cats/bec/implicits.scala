package frameless
package cats

import _root_.cats.implicits._
import _root_.cats._

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object implicits {
  implicit class rddOps[A: ClassTag](lhs: RDD[A]) {
    def csum(implicit m: Monoid[A]): A = lhs.reduce(_ |+| _)
    def cmin(implicit o: Order[A]): A = lhs.reduce(_ min _)
    def cmax(implicit o: Order[A]): A = lhs.reduce(_ max _)
  }

  implicit class pairRddOps[K: ClassTag, V: ClassTag](lhs: RDD[(K, V)]) {
    def csumByKey(implicit m: Monoid[V]): RDD[(K, V)] = lhs.reduceByKey(_ |+| _)
    def cminByKey(implicit o: Order[V]): RDD[(K, V)] = lhs.reduceByKey(_ min _)
    def cmaxByKey(implicit o: Order[V]): RDD[(K, V)] = lhs.reduceByKey(_ max _)
  }
}

object union {
  implicit def unionSemigroup[A]: Semigroup[RDD[A]] =
    new Semigroup[RDD[A]] {
      def combine(lhs: RDD[A], rhs: RDD[A]): RDD[A] = lhs union rhs
    }
}

object inner {
  implicit def pairwiseInnerSemigroup[K: ClassTag, V: ClassTag: Semigroup]: Semigroup[RDD[(K, V)]] =
    new Semigroup[RDD[(K, V)]] {
      def combine(lhs: RDD[(K, V)], rhs: RDD[(K, V)]): RDD[(K, V)] =
        lhs.join(rhs).mapValues { case (x, y) => x |+| y }
    }
}

object outer {
  implicit def pairwiseOuterSemigroup[K: ClassTag, V: ClassTag](implicit m: Monoid[V]): Semigroup[RDD[(K, V)]] =
    new Semigroup[RDD[(K, V)]] {
      def combine(lhs: RDD[(K, V)], rhs: RDD[(K, V)]): RDD[(K, V)] =
        lhs.fullOuterJoin(rhs).mapValues {
          case (Some(x), Some(y)) => x |+| y
          case (None, Some(y)) => y
          case (Some(x), None) => x
          case (None, None) => m.empty
        }
    }
}
