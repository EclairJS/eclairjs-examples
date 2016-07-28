EclairJS Node Express Examples
===================
Examples of how to use EclairJS Node in a Express application

Learn more about the larger [EclairJS project](http://www.eclairjs.org).

## Installation

````
$ npm install
````

## Downloading Data for Examples
The examples use large datasets that need to be download and accessible by your Spark Cluster.
* Download census data
 ** [ss13husa.csv](http://www2.census.gov/acs2013_1yr/pums/csv_pus.zip) and unzip in a location that is accessible by Spark.
 ** [ss13husb.csv](http://www2.census.gov/acs2013_1yr/pums/csv_hus.zip) and unzip in a location that is accessible by Spark.
 ** [states.csv](https://raw.githubusercontent.com/jadianes/spark-r-notebooks/master/applications/exploring-maps/states.csv)


## Usage

```
export JUPYTER_HOST=<ip address>
export HOUSING_DIR=<location of /data/csv_hus this directory needs to contain files ss13husa.csv, ss13husb.csv and states.csv>
export SPARK_MASTER=<ip address ex. spark://10.122.193.195:7077>
node app.js
```

Open a browser to http://localhost:3000

## Deploying to BlueMix
Update manifest.yml with your VCAP environment variables for your BlueMix NodeJS application.
````
cf push
````