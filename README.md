# spark-flow

This is a library for organizing batch processing pipelines in spark and handle automatic checkpointing of intermediate results. The core type is a DC (Distributed Collection) which is analagous to a spark Dataset. DCs have an api encompassing RDDs, Datasets, and Dataframes.

A logical pipeline can be constructed entirely lazily without a spark context. Pass in a spark context to any DC to get the corresponding Dataset, RDD or Dataframe.

## Building Locally
```sbt publishLocal```

## Include in project
```libraryDependencies += "com.bloomberg" %% "spark-flow" % "0.1.0" ```


## Upcoming
* graphx support
* DAG viewer frontend attached to running process
* component / pipeline abstractions
* debug run mode with auto Try wrapped functions and trapped failures
