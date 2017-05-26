package frameless

import org.apache.spark.sql.SparkSession

sealed abstract class Job[A](implicit spark: SparkSession) { self =>
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
        spark.sparkContext.setLocalProperty(key, value)
        self.run()
      }
    }
  }

  def map[B](fn: A => B): Job[B] = new Job[B]()(spark) {
    def run(): B = fn(Job.this.run())
  }

  def flatMap[B](fn: A => Job[B]): Job[B] = new Job[B]()(spark) {
    def run(): B = fn(Job.this.run()).run()
  }
}


object Job {
  def apply[A](a: => A)(implicit spark: SparkSession): Job[A] = new Job[A] {
    def run(): A = a
  }
}
