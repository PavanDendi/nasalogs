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
The docker image used contains Apache Spark as well as the Apache Toree kernel for Jupyter.

Run the following command to start Jupyter:
```
docker run --rm -d -p 8888:8888 -v "$PWD"/jupyter:/home/jovyan/work \
jupyter/all-spark-notebook:437f1ef92733 \
start-notebook.sh --NotebookApp.token=''
```
Navigate to `https://localhost:8888` in a browser, and open `EDA.ipynb` inside the `work` folder.
