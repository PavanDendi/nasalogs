# Problem Statement
Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.  Package the application in a docker container.

# Assumptions
* User has access to git and docker.
* User has sufficient space for the required docker images.
* User's machine is in a secure environment where opening local ports is acceptable.
* User will be running spark locally and not in a cluster.
* User has cloned this repository, and it is the current working directory.

# Exploratory Data Analysis
I used a Jupyter notebook to explore the log file and figure out the main logic for the program.  

The notebook is available for static browsing directly on github:  
https://github.com/PavanDendi/nasalogs/blob/master/jupyter/EDA.ipynb

My local dev environment can also be replicated to run the notebook interactively.  
The docker image used contains Apache Spark as well as the Apache Toree kernel for Jupyter.

Run the following command from the repo base directory to start Jupyter:
```
docker run --rm -p 8888:8888 -v "$PWD"/jupyter:/home/jovyan/work jupyter/all-spark-notebook:437f1ef92733
```
Navigate to the url shown in the terminal, e.g. `http://127.0.0.1:8888/?token=...`, and open `EDA.ipynb` inside the `work` folder.

# Main Program

## Software Versions
* sbt 1.3.7
* scala 2.11.12
* spark 2.4.3

## Documentation
In addition to this README and code comments, this project uses `scaladoc` to generate compiled HTML documentation.  
Open `doc/index.html` in a browser to view the documentation.

## Overview
`src/main/scala/com/pavandendi/nasalogs`:
* `NasaLogs.scala` - main code
  * Reuses a lot of code from EDA notebook
  * Logic is organized into separate functions to make unit testing easier
  * Requires FTP URL and number of results as command line arguments during `spark-submit`
  * Prints the final result to `stdout`
* `SparkSessionWrapper.scala` - Retreives or creates SparkContext for main code execution

`src/test/scala/com/pavandendi/nasalogs`:
* `NasaLogsSpec.scala` - unit tests
  * Provides test for each custom transformation
  * Uses manually generated sample data where possible
  * Uses `scalatest` and `spark-fast-tests` frameworks
* `SparkSessionTestWrapper.scala` - Creates local SparkContext configured for unit testing

# Execution and Unit Tests
Execute the project by running the docker image.  
The image is preconfigured with the correct FTP URL and a `topN` value of 3.  
Please note that the NASA FTP server is sometimes nonresponsive, causing the program to fail occasionally.
```
docker run --rm registry.gitlab.com/pavandendi/nasalogs/nasalogs:0.0.1
```
You can also specify a custom spark-submit string to change the `topN` value.
```
docker run --rm registry.gitlab.com/pavandendi/nasalogs/nasalogs:0.0.1 \
spark-submit --master=local --class=com.pavandendi.nasalogs.NasaLogs \
/nasalogs_2.11-0.0.1.jar $dataURL $topN
```

## Run Unit Tests
Tests can be run similarly by running the build configuration of the docker image.
```
docker run --rm -it registry.gitlab.com/pavandendi/nasalogs/nasalogs-build:0.0.1 sbt test
```

# Dockerfile
The dockerfile is configured as a multistaged build, and is designed with the intention of integration into a CI/CD pipeline.  
The build configuration compiles the source code, runs unit tests, and produces the final jar file.
```
docker build --target build -t $tagname:$version .
```

The release configuration packages the jar file into an image designed to run Spark locally.  
The build configuration is a dependency, so a release image cannot be created without correct compilation and passing all unit tests.  
The base image could be easily switched out for one that is ready to deploy into a cluster.
```
docker build --target release -t $tagname:$version .
```