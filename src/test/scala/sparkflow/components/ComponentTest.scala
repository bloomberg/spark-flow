package sparkflow.components

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._
import sparkflow.layer.PD
import sparkflow.FlowFuncs._

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{LDAModel, LDA}

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by ngoehausen on 3/3/16.
  */
class ComponentTest extends FunSuite with SharedSparkContext with ShouldMatchers{

  test("basicComponent"){

    case class InBundle(nums: PD[Int]) extends Bundle
    case class OutBundle(lt5: PD[Int], gt5: PD[Int]) extends Bundle

    val bundle = InBundle(parallelize(1 to 10))

    println(bundle.calcElements())

    class TestComp(in: InBundle) extends Component[InBundle, OutBundle](in: InBundle){

      def run() = {
        val lt5 = in.nums.filter(_ < 5)
        val gt5 = in.nums.filter(_ > 5)
        OutBundle(lt5, gt5)
      }
    }

    val comp = new TestComp(bundle)

    1 to 4 should contain theSameElementsAs comp.output.lt5.toRDD(sc).collect()
    6 to 10 should contain theSameElementsAs comp.output.gt5.toRDD(sc).collect()

  }

  test("machineLearning"){

    /* Regular spark

    val randomVecs = sc.parallelize(1 to 100).map(i => Vectors.dense(Seq.fill(10)(Random.nextDouble()).toArray))
    val corpus = randomVecs.zipWithUniqueId().map{case (k,v) => (v,k)}
    val ldaModel = new LDA().setK(3).run(corpus)
    println(ldaModel)
     */

    // definitions

    case class CorpusBundle(corpus: PD[(Long, Vector)]) extends Bundle

    class CorpusGenerator() extends Component[Null, CorpusBundle](null){

      def run() = {
        val randomVecs = parallelize(1 to 100).map(i => Vectors.dense(Seq.fill(10)(Random.nextDouble()).toArray))
        val corpus = randomVecs.zipWithUniqueId().map{case (k,v) => (v,k)}
        CorpusBundle(corpus)
      }

    }

    case class ModelOutput(ldaModel: LDAModel) extends Bundle

    implicit def pdToRDD[T: ClassTag](pd: PD[T]): RDD[T] = pd.toRDD(sc)

    class LDAComponent(corpusBundle: CorpusBundle) extends Component[CorpusBundle, ModelOutput](corpusBundle) {

      def run() = {
        ModelOutput(new LDA().setK(3).run(corpusBundle.corpus))
      }

    }

    // top level execution
      val corpusGenerator = new CorpusGenerator()
      val lDAComponent = new LDAComponent(corpusGenerator.output)
      println(lDAComponent.output.ldaModel.topicsMatrix)
    }

}
