package org.apache.dsext.spark.datasource.rest

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.scalatest.{BeforeAndAfterEach, FlatSpec}


class SodaPrepTest extends FlatSpec with BeforeAndAfterEach with DataFrameSuiteBase {
  private val port = 8080
  private val hostname = "localhost"
  // Run wiremock server on local machine with specified port.
  private val wireMockServer = new WireMockServer(wireMockConfig().port(port))

  override def beforeEach {
    wireMockServer.start()
  }

  override def afterEach {
    wireMockServer.stop()
  }

  val response = Map(
    "source" -> "nn",
    "region" -> "Nevada",
    "version" -> "9"
  )

  "Your Client" should "send proper request" in {
      val path = s"/soda/9"
      // Configure mock server stub response
      // json4s is useful to constructing response string if the response is JSON
      implicit val formats = DefaultFormats
      wireMockServer.stubFor(
        get(urlPathEqualTo(path))
          .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(Serialization.write(response))
            .withStatus(200)))

      // Send request by using your HTTP client

    val sodauri = "http://" + hostname + ":" + port  + "/soda/{version}/"


    ///Say we need to call the API for 3 sets of input parameters for different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the SODA API for Socrata data source

    val sodainput1 = ("Nevada", "nn", 12 )
    val sodainput2 = ("Northern California", "pr", 13)
    val sodainput3 = ("Virgin Islands region", "pr",14)

    // Now we create a RDD using these input parameter values

    val sodainputRdd = spark.sparkContext.parallelize(Seq(sodainput1, sodainput2, sodainput3))

    import spark.implicits._

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






      // Verify the request is valid
      wireMockServer.verify(
        getRequestedFor(urlPathEqualTo(path))
          //.withHeader("Content-Type", matching("application/json")))
          .withHeader("Content-Type", equalTo("application/json")))
  }

  // Assert response body itself if necessary
}

