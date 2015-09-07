/**
 * Created by thuy on 31/08/2015.
 */

import java.io._

import scala.collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{ SparseVector => SV }

object TFIDFExample {
  def main(args: Array[String]) {
    val stopwords = scala.io.Source.fromFile("/home/thuy/IdeaProjects/sp/english.stop").getLines().toList
    val conf = new SparkConf().setAppName("LDA").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
	val dict: HashMap[Long,String] = new HashMap() 
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/home/thuy/data/input.csv")
	val documents = df.select("content").rdd.map(
		_.mkString.split(" ")
		.map(_.toLowerCase)
		.filter(!_.matches("""\W+"""))
		.toSeq)
	val company = df.select("company_name").rdd.zipWithIndex.collect
	val company_index: HashMap[Long,String] = new HashMap()
	company.foreach{
		case (row,index) => 
			company_index.put(index,row.mkString)
	}
    val hashingTF = new HashingTF()
	val tf: RDD[Vector] = hashingTF.transform(documents)
	tf.cache()
	val words = documents.collect.flatten.distinct
	words.foreach{
		case word => 
			dict.put(hashingTF.indexOf(word),word)
	}
	val idf = new IDF().fit(tf)
	val tfidf: RDD[Vector] = idf.transform(tf)
	//val out = new FileWriter("TF_IDF.txt", true)
	var i: Int = 0


	//val fw = new FileWriter("TF_IDF.txt", true)
    //    println("HashTag => " + rdd)
    //    fw.write(rdd + "\n")
     //   fw.close()

	//Some(new PrintWriter("filename")).foreach{ 
		//out => 
			tfidf.foreach{
			case document =>
				//val out = new FileWriter(s"TF_IDF_${i}.txt", true)
				println(company_index(i)+"\n")
				i += 1
				val d = document.asInstanceOf[SV]
				val l = d.indices.zip(d.values).sortBy(-_._2).take(50)
				l.foreach{ case (hashcode,value) =>
					try{
						println(dict(hashcode)+": "+value)		
					}		
					catch {
					  case e:Exception => println(e)
					}
				}
				println()
			}
		//out.close()

	//}

  }
}
