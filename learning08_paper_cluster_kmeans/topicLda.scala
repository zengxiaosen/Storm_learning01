/**
  * Created by Administrator on 2016/11/8.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{Level, Logger}
import getTfIdf.documentTermMatrix
import org.apache.spark.sql.SQLContext
object topicLda {

  def main(args: Array[String]): Unit = {
    val scores = Map("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8,"Alice" -> 10)
    print(scores.keySet)
  }
}
