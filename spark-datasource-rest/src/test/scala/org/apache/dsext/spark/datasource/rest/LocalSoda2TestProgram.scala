package org.apache.dsext.spark.datasource.rest

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession


object LocalSoda2TestProgram {

  def main(args: Array[String]): Unit =  {
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();
    import spark.implicits._

    // Create the target url string for Soda API for Socrata data source
    val sodauri = "http://localhost:4567/soda/{version}/"


      ///Say we need to call the API for 3 sets of input parameters for different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the SODA API for Socrata data source

    val sodainput1 = ("Nevada", "nn", 12 )
    val sodainput2 = ("Northern California", "pr", 13)
    val sodainput3 = ("Virgin Islands region", "pr",14)

    // Now we create a RDD using these input parameter values

    val sodainputRdd = spark.sparkContext.parallelize(Seq(sodainput1, sodainput2, sodainput3))

    // Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
    val sodainputKey1 = "region"
    val sodainputKey2 = "source"
    val sodainputKey3 = "version"

    val sodaDf = sodainputRdd.toDF(sodainputKey1, sodainputKey2, sodainputKey3)

    // And we create a temporary table now using the sodaDf
    sodaDf.createOrReplaceTempView("sodainputtbl")

    // Now we create the parameter map to pass to the REST Data Source.

    val parmg = Map("url" -> sodauri, "input" -> "sodainputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")

    // Now we create the Dataframe which contains the result from the call to the Soda API for the 3 different input data points
    val sodasDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

    // We inspect the structure of the results returned. For Soda data source it would return the result in array.
    sodasDf.printSchema

    // Now we are ready to apply SQL or any other processing on teh results

    sodasDf.createOrReplaceTempView("sodastbl")

    //spark.sql("select source, region, inline(output) from sodastbl").show()
    spark.sql("select source, region, version from sodastbl").show()
 




      spark.stop()
  }
}

