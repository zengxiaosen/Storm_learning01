/**
  * Created by Administrator on 2016/11/8.
  */

 import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.linalg.{Vector, Vectors}
  import org.apache.spark.rdd.RDD

  import scala.collection.{Map, mutable}
  import scala.collection.mutable.HashMap

  /**
    * Created by Administrator on 2016/11/8.
    */
  object getTfIdf {
    def documentTermMatrix(docs: RDD[((String, Int), Seq[String])], sc:SparkContext, numTerms: Int = Math.pow(2, 15).toInt):
    (RDD[Vector], Map[Int, String], Map[Long, Int], Map[String, Double]) = {
      /*
      参数: docs:RDD[(artcile_id, Seq[分词])], sc:SparkContext, numTerms:Int 词特征数, 默认为pow(2,15)
      返回: tfidf向量, 词袋映射, 文档id映射, 逆文档数
       */
      // 计算词频 docTermFreqs: RDD[((String,Int), HashMap[String, Int])]
      val docTermFreqs = documentTermFrequencies(docs).cache()

      // 文档序号 docIds:Map[Long, Int]
      val docIds = docTermFreqs.map(_._1._2).zipWithIndex().map(_.swap).collectAsMap()

      // 保留numTerms数量的词汇, 并统计文档数 docFreqs:Array[(String, Int)]
      val docFreqs = documentFrequenciesDistributed(docTermFreqs, sc, numTerms)
      println("Number of terms: " + docFreqs.length)

      //文档数
      val numDocs = docIds.size

      //逆文档数 Map[String, Double]
      val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

      //词项表示为ID termIds: Map[String, Int]
      val idTerms = idfs.keys.zipWithIndex.toMap
      val termIds = idTerms.map(_.swap)//int string

      //广播 idfs逆文档数和 词项MapidTerms
      val bIdfs = sc.broadcast(idfs).value
      val bIdTerms = sc.broadcast(idTerms).value//string int

      //词频逆文档vecs(RDD[Vector])
      val vecs = docTermFreqs.map(_._2).map(termFreqs => {
        //该文档的总词数
        val docTotalTerms = termFreqs.values.sum
        //计算该文档词的词频逆文档数
        val termScores = termFreqs.filter{
          case (term, freq) => bIdTerms.contains(term)
        }.map{
          //词频逆文档数，对文档总词数归一化，词转化为id
          case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
        }.toSeq
        //转化为稀疏向量
        Vectors.sparse(bIdfs.size, termScores)
      })

      //返回: tfidf向量, 词袋映射, 文档id映射, 逆文档数
      (vecs,termIds,docIds,idfs)
    }

    def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int): Map[String, Double] = {
      /*
      Function:计算逆文档数
      参数: 文档频率，文档数
      返回: Map[词，逆文档数]
       */
      docFreqs.map{case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
    }

    def documentTermFrequencies(docs: RDD[((String, Int), Seq[String])]) : RDD[((String, Int), HashMap[String, Int])] = {
      /*
      function: 计算词频
      参数: docs RDD[(标题， 分词)]
      返回: 词频
       */
      docs.mapValues(terms => {
        val termFreqsInDoc = terms.foldLeft(new mutable.HashMap[String, Int]()){
          (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
            map
        }
        termFreqsInDoc
      })
    }

    //计算文档数
    def documentFrequenciesDistributed(docTermFreqs: RDD[((String, Int), HashMap[String, Int])], sc:SparkContext, numTerms: Int) : Array[(String, Int)] = {
      /*
      Function: 分布式计算文档数
      参数: 词频数，保留的特征词数量numTerms
      返回: Array[词，文档数]
       */
      //过滤掉只出现一次的词
      val docFreqs = docTermFreqs.flatMap(_._2.keySet).map((_,1)).reduceByKey(_+_, 15).filter(_._2 > 5)
      //得到特征词
      val topTerms = sc.parallelize(informationGain(docTermFreqs, docFreqs, numTerms))
      docFreqs.join(topTerms).map(x => (x._1, x._2._1)).collect()
    }

    //由信息增益选择特征词
    def informationGain(docTermFreqs: RDD[((String, Int), HashMap[String, Int])], docFreqs: RDD[(String, Int)], numTerms: Int) : Array[(String, Double)] = {
      //词出现的文档数
      val docF = docFreqs.collectAsMap()
      // ((class, article_id), num)
      val termToClass = docTermFreqs.flatMap(x => x._2.keys.map(t => ((t, x._1._1),1))).reduceByKey(_+_).filter{case ((term, classt), frq) => docF.keySet.contains(term)}
      // 类别的文档数 (class, docNum)
      val classF = docTermFreqs.map(x => (x._1._1, 1)).reduceByKey(_+_).collectAsMap()
      // 总文档数
      val numDoc = docTermFreqs.count
      //计算每个词的信息增益，考虑词出现和没出现的情况
      val termIg = termToClass.map{
        case ((term, classt), num) =>
          //特征词存在
          //出现该词的文档数/总文档数
          val t0 = docF(term).toDouble
          val t1 = t0 / numDoc
          //出现词term且属于该类别的文档数/出现term的总文档数
          val t2 = (num / t0) * Math.log(num / t0) / Math.log(2)
          val t = t1 * t2

          //特征词不存在
          var f = 0.0
          val rnum = classF(classt) - num // 不含有term但属于该类别的文档数
          if(num != 0){
            val f0 = numDoc - docF(term).toDouble //没出现term的总文档数
            val f1 = f0 / numDoc
            val f2 = (rnum / f0) * Math.log(rnum / f0) / Math.log(2)
            f = f1 * f2
          }
          (term, t + f)
      }.reduceByKey(_+_)
      //按信息增益大小排序
      //System.setProperty("java.util.Arrays.useLegacyMergeSort","true")
      val ordering = Ordering.by[(String, Double), Double](_._2)
      termIg.top(numTerms)(ordering)
    }




  }



