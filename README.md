# gfw-country-pages-analysis-2

This repo tabulates statistics for GLAD, Terra I and fires datasets in all of our country-pages geographies (gadm36, wdpa, mining, etc), then saves them to the API for use on various platform websites.

Here's a visualization of one of these summary tables-- the graph of weekly GLAD alerts in Brazil on the GFW Climate:
http://climate.globalforestwatch.org/insights/glad-alerts/BRA

### General workflow
We take alert point data (GLAD, Terra I, fires) and run a hadoop process to add iso/adm1/adm2 information to it. The [hadoop_pip repo](https://github.com/wri/hadoop_pip/) does the hard geospatial work, and then we pull the results down for postprocessing. Once formatted properly, we save the results as CSV on S3, then use those tables to update GFW API datasets.

##### Point data
CSV files of alerts (GLAD, TerraI, fires), stored in folders on S3. GLAD data (for example) is here: s3://gfw2-data/alerts-tsv/glad/

##### Polygon data
TSV files of polygon geometry and attributes. Created by our [vector-to-tsv process](https://github.com/wri/gfw-annual-loss-processing/tree/master/1b_Vector-to-TSV). These files don't update very often- usually yearly when we do our loss update. GADM36 boundaries are here: s3://gfw2-data/alerts-tsv/country-pages/admin-only/

### Understanding the config spreadsheet
Config information is stored in [this google sheet](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=20694842). If you don't have access, please try logging in with the wriforests@gmail.com credentials.

##### Hadoop tab
This tab maps forest datasets (GLAD, Terra I, fires) and environments (staging/prod) to config files. These config files are stored in this repo, and have info about the point and polygon files we want to intersect for this dataset.

##### API endpoint lookup tab
This tab stores information about the final output from this process. After we pull the hadoop output down and postprocess, we use this tab (and our combination of dataset / environment / summary type) to determine where to push the final CSV, and what dataset id to run a `data-overwrite` on when we're ready. This tab is also a helpful reference point for Vizzuality to make sure we're both using the same dataset IDs.

### Stepping through the process

##### Kickoff the process
`python update_country_stats.py -d umd_landsat_alerts -e prod`

##### Grab the correct .ini file based on dataset and environment
The `hadoop` tab of our config spreadsheet points us to `glad_prod.ini`, which currently looks like this:

```
; hadoop specific config setting
[reduce]
size = 1.0

; add polygon directory and specify what fields we want to use
[polygons]
path = s3://gfw2-data/alerts-tsv/country-pages/admin-only/
wkt = 0
fields = 1,2,3,6,7,8

; same for points, also specify that this is a CSV
[points]
path = s3://gfw2-data/alerts-tsv/glad/
x = 0
y = 1
fields=0,1,2,3,4,5,6,7
sep = ,

; store the output locally on HDFS, we'll export it from there
[output]
path = hdfs:///user/hadoop/output
sep = ,

; after we've done the spatial join, execute this SQL
; this will GROUP BY our fields of interest, summing area of alerts + emissions
[sql]
query1 = SELECT _c2, _c3, _c4, sum(_c5), sum(_c6), _c7, _c8, _c9, _c10, _c11, _c12, _c13, count(*) FROM my_table GROUP BY _c2, _c3, _c4, _c7, _c8, _c9, _c10, _c11, _c12, _c13

; will generate a full table (before GROUP BY) export
[export_type]
name = glad
```

##### Start hadoop_pip using this config
We'll then start a hadoop job using the above config. We'll use the [hadoop_pip](https://github.com/wri/hadoop_pip) repo to intersect the points path and polygons path defined above, execute the SQL statement on the resulting table, then return the CSV for local processing.

##### Summarize results
This summary process varies by dataset, most just want to count alerts by day, then group to iso, adm1, or adm2. Some (climate) have sophisticated postprocessing required.

##### Push to API
These output tables are pushed to S3. The code then makes POST requests to the API to use them to overwrite existing datasets.

### Automatic processing
Scheduled tasks on the Data Management server currently run the fires_report dataset nightly, and the fires_country_pages dataset every Monday. Simple .cmd files for these batch tasks are in the utilities directory.

