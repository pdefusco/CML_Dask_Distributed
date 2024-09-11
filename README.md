# CML Dask Distributed

### Scale your pandas workflow by changing a single line of code

Modin uses Ray, Dask or Unidist to provide an effortless way to speed up your pandas notebooks, scripts, and libraries. Unlike other distributed DataFrame libraries, Modin provides seamless integration and compatibility with existing pandas code. Even using the DataFrame constructor is identical.

### References

[Dask Docs](https://modin.readthedocs.io/en/latest/index.html)
[Starting Up a Dask Cluster in CML]()

### CDE Data Generation Steps

```
cde credential create --name dckr-crds-modin --type docker-basic --docker-server hub.docker.com --docker-username $docker_user -v
cde resource create --name ge-runtime-modin --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image -v

cde resource create --name modin-files -v
cde resource upload --name modin-files --local-path 00_big_datagen.py --local-path 00_ts_datagen.py

cde job create --name modin-datagen --type spark --application-file 00_datagen.py --mount-1-resource modin-files --runtime-image-resource-name ge-runtime-modin
cde job run --name modin-datagen --driver-cores 5 --driver-memory "10g" --executor-cores 5 --executor-memory "20g"

cde job create --name ts-datagen --type spark --application-file 00_ts_datagen.py --mount-1-resource modin-files --runtime-image-resource-name ge-runtime-modin
cde job run --name ts-datagen --driver-cores 5 --driver-memory "10g" --executor-cores 5 --executor-memory "20g"

```
