```
docker run --rm -d -p 8888:8888 -v "$PWD":/home/jovyan/work \
jupyter/all-spark-notebook:437f1ef92733 \
start-notebook.sh --NotebookApp.token=''
```
