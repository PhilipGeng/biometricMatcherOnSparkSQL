package TestCase.sparkSQL

/**
 * Created by philippy on 2016/3/29.
 */

import breeze.linalg.{DenseVector=>DV,normalize}
import breeze.util.JavaArrayOps._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector=>vec,Vectors}
import breeze.numerics.acos
case class record(name: Int, little_curv:Array[vec],little_norm:Array[Array[vec]],
                  ring_curv:Array[vec],ring_norm:Array[Array[vec]],
                  middle_curv:Array[vec],middle_norm:Array[Array[vec]],
                  index_curv:Array[vec],index_norm:Array[Array[vec]])
class biometrics(){
  val conf = new SparkConf().setAppName("biometrics").set("spark.network.timeout","600").set("spark.akka.heartbeat.interval","100")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def run(n:Int): Long ={
    val datapoints = 32
    val norms = 32
    val segment = 10
    val numOfSamples = n
    val w_curv = 0.6
    val w_norm = 1-w_curv
    val df = sc.parallelize((0 to numOfSamples)).map { each =>
      val id = each
      val little_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
      val ring_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
      val middle_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
      val index_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
      val little_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
      val ring_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
      val middle_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
      val index_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
      record(id,little_curv,little_norm,ring_curv,ring_norm,middle_curv,middle_norm,index_curv,index_norm)
    }.toDF()
    df.registerTempTable("records")

    val t_little_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
    val t_ring_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
    val t_middle_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
    val t_index_curv = Array.fill(segment){Vectors.dense(dvDToArray(normalize(DV.rand(datapoints))))}
    val t_little_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
    val t_ring_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
    val t_middle_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}
    val t_index_norm = Array.fill(segment){Array.fill(norms){Vectors.dense(dvDToArray(normalize(DV.rand(3))))}}

    var start = System.nanoTime()
    val db = sqlContext.sql("SELECT * FROM records")
    val res = db.map{r=>
      val r_id = r.getAs[Int]("name")
      val r_little_curv = r.getAs[Seq[vec]]("little_curv")
      val r_ring_curv = r.getAs[Seq[vec]]("ring_curv")
      val r_middle_curv = r.getAs[Seq[vec]]("middle_curv")
      val r_index_curv = r.getAs[Seq[vec]]("index_curv")
      val r_little_norm = r.getAs[Seq[Seq[vec]]]("little_norm")
      val r_ring_norm = r.getAs[Seq[Seq[vec]]]("ring_norm")
      val r_middle_norm = r.getAs[Seq[Seq[vec]]]("middle_norm")
      val r_index_norm = r.getAs[Seq[Seq[vec]]]("index_norm")
      val simlc = r_little_curv.zip(t_little_curv).map(x=>new DV(x._1.toArray).t * new DV(x._2.toArray)).sum
      val simrc = r_ring_curv.zip(t_ring_curv).map(x=>new DV(x._1.toArray).t * new DV(x._2.toArray)).sum
      val simmc = r_middle_curv.zip(t_middle_curv).map(x=>new DV(x._1.toArray).t * new DV(x._2.toArray)).sum
      val simic = r_index_curv.zip(t_index_curv).map(x=>new DV(x._1.toArray).t * new DV(x._2.toArray)).sum
      val simln = r_little_norm.flatMap(x=>x).zip(t_little_norm.flatMap(x=>x)).map(x=>acos(new DV(x._1.toArray).t * new DV(x._2.toArray))).sum
      val simrn = r_little_norm.flatMap(x=>x).zip(t_little_norm.flatMap(x=>x)).map(x=>acos(new DV(x._1.toArray).t * new DV(x._2.toArray))).sum
      val simmn = r_little_norm.flatMap(x=>x).zip(t_little_norm.flatMap(x=>x)).map(x=>acos(new DV(x._1.toArray).t * new DV(x._2.toArray))).sum
      val simin = r_little_norm.flatMap(x=>x).zip(t_little_norm.flatMap(x=>x)).map(x=>acos(new DV(x._1.toArray).t * new DV(x._2.toArray))).sum
      val sim = w_curv*(simlc+simrc+simmc+simic)+w_norm*(simln+simrn+simmn+simin)
      (r_id,sim)
    }.max()(new Ordering[Tuple2[Int, Double]]() {
      override def compare(x: (Int, Double), y: (Int, Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })
    val maxid = res._1
    var time = System.nanoTime()-start
    println("time consumption:"+time)
    time
  }
}
