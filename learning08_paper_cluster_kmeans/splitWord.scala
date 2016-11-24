import org.apache.spark.SparkConf

/**
  * Created by Administrator on 2016/11/8.
  */
import java.util.Properties

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions.bufferAsJavaList

import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.ansj.util.FilterModifWord
import org.ansj.app.keyword.KeyWordComputer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}
object splitWord {

  case class ArticleInfo(article_id: Int, path: String, content: String, title: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("family_words")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //读入分词后的文本数据
    val stopSplitWordFile = "D:\\stop_words_ch.txt"
    val saveSplitWordFile = "D:\\article_splitWords.txt"
    val saveWord2Vec = "D:\\wordsForVec"
    val savePredictCategory = "D:\\article_category"

    //读取停用词
    val stopWords = sc.textFile(stopSplitWordFile)
    val dropWords = sc.broadcast(stopWords.collect().toBuffer)
    //取出数据来完成分词
    //Article_id,plate,path,title,content,其中articleid包含string int
    //val article = readArticleInfo()
  }

}
