package com.bloomberg.sparkflow.dc

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.scalatest._

import scala.util.Random
import com.bloomberg.sparkflow._

/**
  * Created by ngoehausen on 3/23/16.
  */
class DRTest extends FunSuite with SharedSparkContext with ShouldMatchers {

  test("normalize"){



    val numbers: DC[Int] = parallelize(1 to 10)
    val doubles: DC[Double] = numbers.map(_.toDouble)
    val sum: DR[Double] = doubles.mapToResult(rdd => rdd.sum())
    val normalized: DC[Double] = doubles.withResult(sum).map{case (number, s) => number / s}

    val normalizedRDD = normalized.getRDD(sc)
    normalizedRDD.foreach(println)
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
    println(ldaModel.get(sc).topicsMatrix)

  }

  test("regularSpark"){
    val numbers: RDD[Int] = sc.parallelize(1 to 10)
    val doubles: RDD[Double] = numbers.map(_.toDouble)
    val sum: Double = doubles.sum()
    val normalized: RDD[Double] = doubles.map(_ / sum)
  }
}
