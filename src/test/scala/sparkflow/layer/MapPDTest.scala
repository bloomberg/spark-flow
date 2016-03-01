package sparkflow.layer

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import sparkflow.FlowFuncs._
import sparkflow.serialization.Formats.CompactPD
import sparkflow.execute.Run.getRDD

/**
  * Created by ngoehausen on 2/29/16.
  */
class MapPDTest extends FunSuite {

  test("basicMap"){
    val pd = parallelize(1 to 10)
    val doubled = pd.map(_ * 2)

    val compactPd = doubled.toCompactPD()
    val str = compactPd.toString()

    println(str)

    val recovered = CompactPD.fromString(str)

    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))
    val rdd = getRDD(recovered, sc)
    rdd.foreach(println)

  }

}
