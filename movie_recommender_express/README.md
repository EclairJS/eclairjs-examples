# EclairJS Movie Recommender Express Example

This example is an implementation of [Building a Movie Recommendation Service with Apache Spark & Flask](https://www.codementor.io/spark/tutorial/building-a-recommender-with-apache-spark-python-example-app-part1) and will show you how to use EclairJS Client in a Express application.  It uses movie ratings assigned by the user to make predictions about other movies that may be of interest to them.  Learn more about the larger [EclairJS project](http://www.eclairjs.org).

## Downloading data for the example
The examples use large datasets that need to be download and accessible by your Spark CLuster.

### Download the [movieLens rating dataset](http://grouplens.org/datasets/movielens/)
* [ml-latest-small.zip](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip) and unzip in a location that is accessible by Spark.
* [ml-latest.zip](http://files.grouplens.org/datasets/movielens/ml-latest.zip) and unzip in a location that is accessible by Spark.

## To run the demo
If docker is running with docker-machine or on a different server, please edit the JUPYTER_HOST variable in docker_env.sh

### 1. Launch the docker environment

```bash
docker pull eclairjs/minimal-gateway
docker run -p 8888:8888 -v <fullpath to location of movielens datasets>:/data eclairjs/minimal-gateway
```

For example, if you unzipped the files to a sub-directory "movielens" under your Downloads directory the docker run command [macOS] would be
```bash
docker run -p 8888:8888 -v /Users/<userid>/Downloads/movielens:/data eclairjs/minimal-gateway
```

### 2. Run the Node.js app

```bash
npm install
. ./docker_env.sh
node --harmony app.js
```

Open a browser to http://localhost:3000

## Deploying to BlueMix
Update manifest.yml with your VCAP environment variables for your BlueMix NodeJS application.
````
cf push
````
