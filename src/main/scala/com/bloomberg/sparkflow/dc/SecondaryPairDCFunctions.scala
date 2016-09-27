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

import com.bloomberg.sparkflow.partitioning.SecondarySortPartioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

/**
  * Created by ngoehausen on 6/6/16.
  */
class SecondaryPairDCFunctions[K, K2, V](self: DC[((K, K2), V)])
                                        (implicit kt: ClassTag[K],
                                         vt: ClassTag[V],
                                         k2t: ClassTag[K2],
                                         ord: Ordering[(K, K2)] = null,
                                         encoder: Encoder[((K, K2), V)]) {

  def repartitionAndSecondarySortWithinPartitions(): DC[((K, K2), V)] = {
    new RDDTransformDC(encoder, self, (rdd: RDD[((K, K2), V)]) => rdd.repartitionAndSortWithinPartitions(new SecondarySortPartioner[K, K2, V](rdd.partitions.length)), Seq("repartAndSort"))
  }

  def repartitionAndSecondarySortWithinPartitions(numPartitions: Int): DC[((K, K2), V)] = {
    new RDDTransformDC(encoder, self, (rdd: RDD[((K, K2), V)]) => rdd.repartitionAndSortWithinPartitions(new SecondarySortPartioner[K, K2, V](numPartitions)), Seq("repartAndSort", numPartitions.toString))
  }
}
