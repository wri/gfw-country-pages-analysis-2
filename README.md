# gfw-country-pages-analysis-2

This repo tabulates statistics for GLAD and Terra I datasets in various geographies, then saves them to the API for use on various platform websites.

For example, the graph of weekly GLAD alerts in Brazil on the GFW Climate:
http://climate.globalforestwatch.org/insights/glad-alerts/BRA

### Kickoff the process
`python update_country_stats.py -l umd_landsat_alerts -e prod`


### Identify polygon datasets based on our input alert data
Code looks at the `hadoop` tab of the [config spreadsheet](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=20694842) to identify what contextual datasets are associated with our input layer (in this case `umd-landsat-alerts`) and our environment (`prod`)

### Start hadoop_pip based on this input list
Uses that list of contextual datasets (for the above, it would be `gadm2_boundary`, `wdpa`, `idn_moratorium`, and `mys_idn_peat`) to run our [hadoop point in polygon code](http://github.com/wri/hadoop-pip) to count alerts in those various boundaries

### Summarize results
After hadoop_pip runs, download the results from S3 and postprocess locally

This varies by dataset, most just want to count alerts by day. Some (climate) have sophisticated postprocessing required

### Push to API
These output tables are pushed to S3. The code then makes POST requests to the API to use them to overwrite existing datasets.

These combinations (alert dataset / contextual dataset / environment to API dataset ID) are managed by the `api_endpoint_lookup` tab in the [gfw-country-pages-2 config doc](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=20694842)
