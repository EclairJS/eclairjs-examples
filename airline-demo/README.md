# Airline Demo

This demo makes use of Spark's streaming libraries.  Live flight data for all US airports for the day is used over a sliding 10 minute window to determine the percentage of on-time take offs and major/minor delays for a selected airport.  

Prereqs:
    - Docker (see https://docs.docker.com/engine/installation/mac/)

## To run the demo.
### 1. Launch the docker environment.

```bash
docker-compose up
```

### 2. Run the Node.js app.

If docker is running with docker-machine or on a different server, please edit the JUPYTER_HOST variable in docker_env.sh

```bash
npm install
. ./docker_env.sh
node --harmony index.js
```


### 3. To restart the demo/docker container:

```bash
docker-compose down
docker-compose up -d
```
