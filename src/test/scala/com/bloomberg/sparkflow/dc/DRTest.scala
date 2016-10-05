/*
 * Copyright 2016 Bloomberg LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    val sum: DR[Double] = doubles.sum
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
    val ldaModel = corpus.mapToResult(rdd => new LDA().setK(3).run(rdd))

  }

  test("regularSpark"){
    val numbers: RDD[Int] = sc.parallelize(1 to 10)
    val doubles: RDD[Double] = numbers.map(_.toDouble)
    val sum: Double = doubles.sum()
    val normalized: RDD[Double] = doubles.map(_ / sum)
  }
}
