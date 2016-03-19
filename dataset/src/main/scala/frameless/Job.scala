package frameless

import org.apache.spark.SparkContext

sealed abstract class Job[A](implicit sc: SparkContext) { self =>
  /** Runs a new Spark job. */
  def run(): A

  def withGroupId(groupId: String): Job[A] = {
    withLocalProperty("spark.jobGroup.id", groupId)
  }

  def withDescription(groupId: String): Job[A] = {
    withLocalProperty("spark.job.description", groupId)
  }

  def withLocalProperty(key: String, value: String): Job[A] = {
    new Job[A] {
      def run(): A = {
        sc.setLocalProperty(key, value)
        self.run()
      }
    }
  }
}

object Job {
  def apply[A](a: => A)(implicit sc: SparkContext): Job[A] = new Job[A] {
    def run(): A = a
  }
}
