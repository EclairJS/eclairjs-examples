# Sales Demo

This demo makes use of spark's machine learning libraries.  A years worth of sales data for a particular product is used to create a linear regression model.  Live sales data is then streamed into the application to test the validity of the model. In this case we are testing the actual sales of that product for the day versus the model's predicted sales.

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
node --harmony app.js
```
