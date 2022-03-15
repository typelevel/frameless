package frameless
package cats

import _root_.cats._
import _root_.cats.kernel.{CommutativeMonoid, CommutativeSemigroup}
import _root_.cats.syntax.all._
import alleycats.Empty

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object implicits extends FramelessSyntax with SparkDelayInstances {
  implicit class rddOps[A: ClassTag](lhs: RDD[A]) {
    def csum(implicit m: CommutativeMonoid[A]): A =
      lhs.fold(m.empty)(_ |+| _)
    def csumOption(implicit m: CommutativeSemigroup[A]): Option[A] =
      lhs.aggregate[Option[A]](None)(
        (acc, a) => Some(acc.fold(a)(_ |+| a)),
        (l, r) => l.fold(r)(x => r.map(_ |+| x) orElse Some(x))
      )

    def cmin(implicit o: Order[A], e: Empty[A]): A = {
      if (lhs.isEmpty()) e.empty
      else lhs.reduce(_ min _)
    }
    def cminOption(implicit o: Order[A]): Option[A] =
      csumOption(new CommutativeSemigroup[A] {
        def combine(l: A, r: A) = l min r
      })

    def cmax(implicit o: Order[A], e: Empty[A]): A = {
      if (lhs.isEmpty()) e.empty
      else lhs.reduce(_ max _)
    }
    def cmaxOption(implicit o: Order[A]): Option[A] =
      csumOption(new CommutativeSemigroup[A] {
        def combine(l: A, r: A) = l max r
      })
  }

  implicit class pairRddOps[K: ClassTag, V: ClassTag](lhs: RDD[(K, V)]) {
    def csumByKey(implicit m: CommutativeSemigroup[V]): RDD[(K, V)] = lhs.reduceByKey(_ |+| _)
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
