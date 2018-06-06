# gfw-country-pages-analysis-2

This repo tabulates statistics for GLAD, Terra I and fires datasets in all of our country-pages geographies (gadm28, wdpa, mining, etc), then saves them to the API for use on various platform websites.

Here's a visualization of one of these summary tables-- the graph of weekly GLAD alerts in Brazil on the GFW Climate:
http://climate.globalforestwatch.org/insights/glad-alerts/BRA

### Kickoff the process
`python update_country_stats.py -d umd_landsat_alerts -e prod`


### Identify polygon datasets based on our input alert data
Code looks at the `hadoop` tab of the [config spreadsheet](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=20694842) to identify what config file we want to run for each combination of layer and environment. In this case (layer `umd-landsat-alerts` and environment `prod`), we want to use the config file `glad_prod.ini`. This is stored in the /config directory. It looks like this:

```
[reduce]
size = 1.0

[polygons]
path = s3://gfw2-data/alerts-tsv/country-pages/ten-by-ten/
wkt = 0
fields = 1,2,3,6,7,8

[points]
path = s3://gfw2-data/alerts-tsv/glad/
x = 0
y = 1
fields=0,1,2,3,4,5,6,7
sep = ,

[output]
path = hdfs:///user/hadoop/output
sep = ,

[sql]
query1 = SELECT _c2, _c3, _c4, sum(_c5), sum(_c6), _c7, _c8, _c9, _c10, _c11, _c12, _c13, count(*) FROM my_table GROUP BY _c2, _c3, _c4, _c7, _c8, _c9, _c10, _c11, _c12, _c13

[export_type]
name = glad
```

### Start hadoop_pip using this config
We'll then start a hadoop job using the above config. We'll use the hadoop_pip repo to intersect the points path and polygons path defined above, execute the SQL statement on the resulting table, then return the CSV for local processing.

### Summarize results
This summary process varies by dataset, most just want to count alerts by day, then group to iso, adm1, or adm2. Some (climate) have sophisticated postprocessing required.

### Push to API
These output tables are pushed to S3. The code then makes POST requests to the API to use them to overwrite existing datasets.

These combinations (alert dataset / environment / summary type to API dataset ID) are managed by the `api_endpoint_lookup` tab in the [gfw-country-pages-2 config doc](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=20694842)
