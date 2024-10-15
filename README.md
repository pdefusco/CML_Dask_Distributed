# CML Dask Distributed

### References

[Dask Docs](https://dask.readthedocs.io/en/latest/index.html)
[Starting Up a Dask Cluster in CML]()

### CDE Data Generation Steps

```
cde credential create --name dckr-crds-dask --type docker-basic --docker-server hub.docker.com --docker-username $docker_user -v
cde resource create --name ge-runtime-dask --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image -v

cde resource create --name dask-files -v
cde resource upload --name dask-files --local-path 00_big_datagen.py --local-path 00_ts_datagen.py --local-path 00_bigdata_join_datagen.py

cde job create --name dask-datagen --type spark --application-file 00_big_datagen.py --mount-1-resource dask-files --runtime-image-resource-name ge-runtime-dask
cde job run --name dask-datagen --driver-cores 5 --driver-memory "10g" --executor-cores 5 --executor-memory "20g"

cde job create --name dask-join-datagen --type spark --application-file 00_bigdata_join_datagen.py --mount-1-resource dask-files --runtime-image-resource-name ge-runtime-dask
cde job run --name dask-join-datagen --driver-cores 5 --driver-memory "10g" --executor-cores 5 --executor-memory "20g"

cde job create --name ts-datagen --type spark --application-file 00_ts_datagen.py --mount-1-resource dask-files --runtime-image-resource-name ge-runtime-dask
cde job run --name ts-datagen --driver-cores 5 --driver-memory "10g" --executor-cores 5 --executor-memory "20g"
```
