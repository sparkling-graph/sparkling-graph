# How to contribute 

New methods and algorithms are essential to make SparklingGraph most used distributed graph processing library. We are not able to keep track on recent papers and implement all methods. Because of that feel free to create pull requests with your implementations. Besides new features feel free to fix any bug that you will find. Also, any piece of code that will be useful for a community (scientists and commercial usage) is welcome. If you do not have Scala coding skills but want your idea (measure,algorithm or other) to be present in library, raise an issue

# Modules

Project is split into couple modules

* API - code parts that can be used to implement extensions (traits and others)
* Loaders - methods used for graph loading
* Operators - measures and algorithms executed on  graph

If your code does not fit in any of them, please mention it in your issue.

## Getting started

* Make sure you have a [GitHub account](https://github.com/signup/free)
* Submit a ticket for your issue, assuming one does not already exist.
  * Clearly describe the problem including steps to reproduce when it is a bug. If it is a feature request than describe exactly what it is
* Fork the repository on GitHub
* Develop your code according to [CleanCode](https://cleancoders.com) and [SOLID](https://en.wikipedia.org/wiki/SOLID_(object-oriented_design)) principle. 


## Making Changes

* Create a topic branch from where you want to base your work.
  * This is usually the master branch.
* Make commits of logical units.
* Check for unnecessary whitespace with `git diff --check` before committing.
* Make sure your commit messages are in the proper format.
* Make sure you have added the necessary tests for your changes.
* Run _all_ the tests to assure nothing else was accidentally broken.
* Add documentation for your feature in [docs](https://github.com/sparkling-graph/sparkling-graph-docs)

## Submitting Changes

* Push your changes to a topic branch in your fork of the repository.
* Submit a pull request to the repository
*  We will look at your request and check it for correctness, also a feedback will be given for you.
* After feedback has been given we expect your response.

Check our [docs](https://github.com/sparkling-graph/sparkling-graph-docs) and [Readme](https://github.com/sparkling-graph/sparkling-graph/blob/master/README.md). Feel free to ask any questions on our [gitter](https://gitter.im/sparkling-graph/sparkling-graph)