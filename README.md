# sparkling-graph

[![Build Status](https://travis-ci.org/sparkling-graph/sparkling-graph.svg?branch=master)](https://travis-ci.org/sparkling-graph/sparkling-graph) [![Documentation Status](https://readthedocs.org/projects/sparkling-graph/badge/?version=latest)](http://sparkling-graph.readthedocs.org/en/latest/?badge=latest) [![Coverage Status](https://coveralls.io/repos/github/sparkling-graph/sparkling-graph/badge.svg?branch=master)](https://coveralls.io/github/sparkling-graph/sparkling-graph?branch=master) [API](http://sparkling-graph.github.io/sparkling-graph/latest/api/) |  [Spark packages](http://spark-packages.org/package/sparkling-graph/sparkling-graph)

# Dependencies
## Snapshot
```
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```
```
libraryDependencies += ml.sparkling % sparkling-graph-loaders % 0.0.5-SNAPSHOT
libraryDependencies += ml.sparkling % sparkling-graph-operators % 0.0.5-SNAPSHOT
```
## Release
```
libraryDependencies += ml.sparkling % sparkling-graph-loaders % 0.0.4
libraryDependencies += ml.sparkling % sparkling-graph-operators % 0.0.4
```

# Current features

* Loading
  * CSV
* Measures - all measures can be configured to treat graphs as directed and undirected
  *  Closeness
  *  Local clustering
  *  Eigenvector
  *  Hits
  *  Neighbor connectivity
  *  Vetex embeddedness
* Experiments
  *  Describe graph using all measures to CSV files

# Planned features
* DSL - easy to use domain specific language that will boost productivity of library
* Loading
  *  GraphML
  *  GML
* Measures
  * Katz
  * Betweenness
* Comunity detection methods
  * Modularity maximization
  * SCAN
  * Infomap 
* API
  *  Random walk
  *  BFS
* ML
  *  Link prediction
  *  Vertex classification
  
# How to

Please check API and examples, in depth docs will be created soon.



# Citation
If you use SparklingGraph in your research and publish it, please consider citing us, it will help us get funding for making the library better.
Currently manuscript is in preparation, so please us following references:

 ``` Bartusiak R. (2016). SparklingGraph: large scale, distributed graph processing made easy. Manuscript in preparation. ```
 
 ```
@unpublished{sparkling-graph
title={SparklingGraph: large scale, distributed graph processing made easy},
author={Bartusiak R.},
note = {Manuscript in preparation},
year = {2016}
}
```
