package sparkflow.layer

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._
import sparkflow.FlowFuncs._

import scala.util.Random

/**
  * Created by ngoehausen on 3/23/16.
  */
class DRTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("normalize"){
    val numbers = parallelize(1 to 10)
    val doubles = numbers.map(_.toDouble)
    val sum = doubles.mapToResult(_.sum())
    val normalized = doubles.mapWith(sum)(_ / _)

    val rdd = normalized.getRDD(sc)
    rdd.foreach(println)
  }


  test("machineLearning"){

    /* Regular spark

    val randomVecs = sc.parallelize(1 to 100).map(i => Vectors.dense(Seq.fill(10)(Random.nextDouble()).toArray))
    val corpus = randomVecs.zipWithUniqueId().map{case (k,v) => (v,k)}
    val ldaModel = new LDA().setK(3).run(corpus)
    println(ldaModel)
     */

    val randomVecs = parallelize(1 to 100).map(i => Vectors.dense(Seq.fill(10)(Random.nextDouble()).toArray))
    val corpus = randomVecs.zipWithUniqueId().map{case (k,v) => (v,k)}
    val ldaModel = corpus.mapToResult(new LDA().setK(3).run)
    println(ldaModel.getResult(sc).topicsMatrix)

  }
}
