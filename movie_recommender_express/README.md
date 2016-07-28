EclairJS Node Express Examples
===================
Examples of how to use EclairJS Node in a Express application

Learn more about the larger [EclairJS project](http://www.eclairjs.org).

## Installation

````
$ npm install
````

## Downloading Data for Examples
The examples use large datasets that need to be download and accessible by your Spark CLuster.
* Download the [movieLens rating dataset](http://grouplens.org/datasets/movielens/)
    **[ml-latest-small.zip](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip) and unzip in a location that is accessible by Spark.
 **[ml-latest.zip](http://files.grouplens.org/datasets/movielens/ml-latest.zip) and unzip in a location that is accessible by Spark.

## Usage

```
export JUPYTER_HOST=<ip address>
export SMALL_DATASET=<location of /data/movielens/ml-latest-small>
export LARGE_DATASET=<location of /data/movielens/ml-latest>
export SPARK_MASTER=<ip address ex. spark://10.122.193.195:7077>
node app.js
```

Open a browser to http://localhost:3000

## Deploying to BlueMix
Update manifest.yml with your VCAP environment variables for your BlueMix NodeJS application.
````
cf push
````