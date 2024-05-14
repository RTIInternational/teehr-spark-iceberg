
```
export pod=spark-iceberg-565b4d767b-jfbx9 
kubectl cp /media/sf_mdenno/Downloads/joined_nwm20_retrospective_2016.parquet  teehr-spark-default/$pod:/opt/spark/
kubectl cp /media/sf_mdenno/Downloads/usgs_nwm21_crosswalk.conus.parquet teehr-spark-default/$pod:/opt/spark/
kubectl cp /media/sf_mdenno/Downloads/nwm21_2016.parquet teehr-spark-default/$pod:/opt/spark/
kubectl cp /media/sf_mdenno/Downloads/usgs_2016.parquet teehr-spark-default/$pod:/opt/spark/
```
