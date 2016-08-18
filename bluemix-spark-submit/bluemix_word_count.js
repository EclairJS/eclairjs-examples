/*
 * Copyright 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 Usage:
 bin/eclairjs-bluemix-spark-submit.sh examples/bluemix_word_count.js"
 */

var SparkConf = require('eclairjs/SparkConf');
var SparkContext = require('eclairjs/SparkContext');
var conf = new SparkConf().setAppName("JavaScript word count");
var sc = new SparkContext(conf);
var Tuple2 = require('eclairjs/Tuple2');

/*
Setup for using IBM BlueMix Object Store.
 */

//var hadoopConf = sparkContext.hadoopConfiguration();
sc.setHadoopConfiguration("fs.swift.service.softlayer.auth.url", "https://identity.open.softlayer.com/v3/auth/tokens");
sc.setHadoopConfiguration("fs.swift.service.softlayer.auth.endpoint.prefix", "endpoints");
sc.setHadoopConfiguration("fs.swift.service.softlayer.tenant", "b5e2b3ed2e44429b0777516795d2d12"); //product id
sc.setHadoopConfiguration("fs.swift.service.softlayer.username", "ab3090cf06a34585aeed150043052836"); //user id
sc.setHadoopConfiguration("fs.swift.service.softlayer.password", "R0fF.Pn~dKbb5HaP"); // password
sc.setHadoopConfiguration("fs.swift.service.softlayer.apikey", "R0fF.Pn~dKbb5HaP"); // password

var rdd = sc.textFile("swift://wordcount.softlayer/dream.txt").cache();

var rdd2 = rdd.flatMap(function (sentence) {
    return sentence.split(" ");
});

var rdd3 = rdd2.filter(function (word) {
    return word.trim().length > 0;
});

var rdd4 = rdd3.mapToPair(function (word, Tuple2) {
    return new Tuple2(word, 1);
}, [Tuple2]);

var rdd5 = rdd4.reduceByKey(function (a, b) {
    return a + b;
});

var rdd6 = rdd5.mapToPair(function (tuple, Tuple2) {
    return new Tuple2(tuple._2() + 0.0, tuple._1());
}, [Tuple2]);

var rdd7 = rdd6.sortByKey(false);

print("top 10 words = " + JSON.stringify(rdd7.take(10)));

sc.stop();




