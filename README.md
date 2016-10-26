# spark-flow

This is a library for organizing batch processing pipelines in Apache Spark and handling automatic checkpointing of intermediate results. The core type is a DC (Distributed Collection) which is analagous to a Spark Dataset. DCs have an API encompassing RDDs, Datasets, and Dataframes.

A logical pipeline can be constructed entirely lazily without a Spark context. Pass in a Spark context to any DC to get the corresponding Dataset, RDD or Dataframe.

## Building Locally
    sbt publishLocal

## Include in project
    libraryDependencies += "com.bloomberg" %% "spark-flow" % "0.1.0"

## Basic Example
    
    import com.bloomberg.sparkflow
    
    val numbers = sparkflow.parallelize(1 to 10)
    val filtered = numbers.filter(_ < 3).checkpoint()
    val doubled = filtered.map(_ * 2)
    
    println(doubled.getRDD(sc).sum())
    
## Combined Dataframe API
    import com.bloomberg.sparkflow
    
    val cars = sparkflow.read
      .format("csv")
      .option("header", "true")
      .load(testFile("cars.csv"))

    val makeModel = cars.select("make", "model").checkpoint()
    
    makeModel.getDF(sc).show()
    
## Larger Example

    object FilmsPipeline {
  
      class FilmMain(){
  
        val inputFilmRows = sparkflow.read.format("csv").option("header", "true")
          .load(testFile("Film_Locations_in_San_Francisco.csv"))
          .toDF(FilmsPipeline.columns:_*)
          .as[FilmsPipeline.InputFilmRow]
  
        val filmRows = inputFilmRows.keyBy(_.title)
          .groupByKey()
          .map(parseFilmRows)
          .checkpoint()
  
        val actorMovieCount = filmRows.flatMap(filmRow => filmRow.actors.map((_,1))).reduceByKey(_+_)
        val topActors = actorMovieCount.sortBy(_._2, ascending = false).map(_._1).take(5)
  
        val filmsWithTopActors = filmRows.withResult(topActors).filter{
          filmRowActors => {
            val (filmRow, actors) = filmRowActors
            filmRow.actors.toSet.intersect(actors.toSet).nonEmpty
          }
        }.map(_._1)
      }
  
  
      val columns = Seq(
        "title",
        "release",
        "locations",
        "funFacts",
        "productionCompany",
        "distributor",
        "director",
        "writer",
        "actor1",
        "actor2",
        "actor3"
      )
  
      case class InputFilmRow(title: String,
                              release: String,
                              locations: String,
                              funFacts: String,
                              productionCompany: String,
                              distributor: String,
                              director: String,
                              writer: String,
                              actor1: String,
                              actor2: String,
                              actor3: String)
  
      case class FilmRow( title: String,
                          release: String,
                          locations: Seq[String],
                          funFacts: String,
                          productionCompany: String,
                          distributor: String,
                          director: String,
                          writer: String,
                          actors: Seq[String])
  
  
      val parseFilmRows = (tuple: (String, Seq[InputFilmRow])) => {
        val (title, rows) = tuple
        val firstRow = rows.head
        val locations = rows.map(_.locations).distinct
        val actors = rows.flatMap(row => Seq(row.actor1, row.actor2, row.actor3)).distinct.filter(_ != "")
        FilmRow(
          firstRow.title,
          firstRow.release,
          locations,
          firstRow.funFacts,
          firstRow.productionCompany,
          firstRow.distributor,
          firstRow.director,
          firstRow.writer,
          actors)
      }
  
      def testFile(fileName: String): String = {
        Thread.currentThread().getContextClassLoader.getResource(fileName).toString
      }
  
    }



## Upcoming
* graphx support
* DAG viewer frontend attached to running process
* component / pipeline abstractions
* debug run mode with auto Try wrapped functions and trapped failures
