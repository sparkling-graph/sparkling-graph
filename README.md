# sparkling-graph
[![Build Status](https://travis-ci.org/sparkling-graph/sparkling-graph.svg?branch=master)](https://travis-ci.org/sparkling-graph/sparkling-graph) [![Documentation Status](https://readthedocs.org/projects/sparkling-graph/badge/?version=latest&cache=1234)](http://sparkling-graph.readthedocs.org/en/latest/?badge=latest) [![codecov.io](https://codecov.io/github/sparkling-graph/sparkling-graph/coverage.svg?branch=master)](https://codecov.io/github/sparkling-graph/sparkling-graph?branch=master) [![Codacy Badge](https://api.codacy.com/project/badge/grade/9ddff907e39a431485fecaf0f612a528)](https://www.codacy.com/app/riomus/sparkling-graph) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/ml.sparkling/sparkling-graph-examples_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ml.sparkling/sparkling-graph-examples_2.11) [![MLOSS](https://img.shields.io/badge/MLOSS-0.0.5-brightgreen.svg)](https://mloss.org/software/view/650/) [![Spark Packages](https://img.shields.io/badge/Spark%20Packages-0.0.5-brightgreen.svg)](http://spark-packages.org/package/sparkling-graph/sparkling-graph) [![API](https://img.shields.io/badge/API-latest-brightgreen.svg)](http://sparkling-graph.github.io/sparkling-graph/latest/api/) [![Gratipay Team](https://img.shields.io/gratipay/team/sparklinggraph.svg)](https://gratipay.com/sparklinggraph/) [![Gitter](https://badges.gitter.im/sparkling-graph/sparkling-graph.svg)](https://gitter.im/sparkling-graph/sparkling-graph?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

SparklingGraph provides easy to use set of features that will give you ability to proces large scala graphs using Spark and GraphX.

# Dependencies
## Snapshot
```
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```
```
// one or all from:
libraryDependencies += "ml.sparkling" %% "sparkling-graph-examples" % "0.0.6-SNAPSHOT"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-loaders" % "0.0.6-SNAPSHOT"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.6-SNAPSHOT"
```
## Release

```
// one or all from:
libraryDependencies += "ml.sparkling" %% "sparkling-graph-examples" % "0.0.5"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-loaders" % "0.0.5"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.5"
```

# Current features

* Loading
  * Formats: 
    * CSV
    * GraphML
  * DSL
* Measures -  measures can be configured to treat graphs as directed and undirected
  * Measures DSL - easy to use domain specific language that boost productivity of library
  * Graph
    * Modularity
    * Freeman's network centrality
  * Vertex
    *  Closeness
    *  Local clustering
    *  Eigenvector
    *  Hits
    *  Neighbor connectivity
    *  Vertex embeddedness
  * Edges
    * Adamic/Adar
    * Common neighbours
* Comunity detection methods
  * PSCAN (SCAN)
* Link prediction
  * Similarity measure based
* Experiments
  *  Describe graph using all measures to CSV files

# Planned features
* Loading
  *  GML
* Measures
  * Katz
  * Betweenness
* Comunity detection methods
  * Modularity maximization
  * Infomap 
* API
  *  Random walk
  *  BFS
* ML
  *  Vertex classification
  
# How to

Please check [API](http://sparkling-graph.github.io/sparkling-graph/latest/api/), [examples](https://github.com/sparkling-graph/sparkling-graph/tree/master/examples/src/main/scala/ml/sparkling/graph/examples) or [docs](http://sparkling-graph.readthedocs.org/en/latest/)



# Citation
If you use SparklingGraph in your research and publish it, please consider citing us, it will help us get funding for making the library better.
Currently manuscript is in preparation, so please us following references:

 ``` Bartusiak et al. (2016). SparklingGraph: large scale, distributed graph processing made easy. Manuscript in preparation. ```
 
 ```
@unpublished{sparkling-graph
title={SparklingGraph: large scale, distributed graph processing made easy},
author={Bartusiak R., Kajdanowicz T.},
note = {Manuscript in preparation},
year = {2016}
}
```
