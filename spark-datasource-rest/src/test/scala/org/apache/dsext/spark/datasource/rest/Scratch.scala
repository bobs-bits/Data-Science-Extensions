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
import java.net.URLEncoder

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object Scratch {

  def main(args: Array[String]): Unit =  {

    val url = "http://hostname:port/some{var1}/some/{var2}/isGood?{var3}"

    //go through url, split around path params to get keys, sub in keys matching values.

    //I want a fast way to pop values into bits of a string.
    // it should work as though I have a into


    //var path_params_tmp = scala.collection.mutable.Map[String, Int]()
    //var path_params_map : Map[String,Int] = Map()
    val path_params_map = collection.mutable.Map[String, Int]()

    var buff = ArrayBuffer[Char]()
    var chunks = ArrayBuffer[String]()


    var inBraces : Boolean = false
    for (c <- url) {
      println("checking char "+ c)
      if(c == '{') {
        println("got left {")
        if (buff.length > 0) {
          chunks += buff.mkString("")
          buff.clear
        }
        inBraces = true;
      }
      else if(c == '}' ) {
        println("got right }")
        inBraces = false;
        chunks += buff.mkString("")
        path_params_map +=  ( chunks(chunks.length-1)-> (chunks.length-1))
        buff.clear()
      }
      else {
        println("processing char "+ c)
        buff += c;
      }
    }

    if(inBraces == true){
      throw new RuntimeException("unmatched left curly brace in url:" + url);
    }

    //add any left over buff
    if(buff.length > 0){
      chunks += buff.mkString("")
    }


    println("chunks")
    println(chunks.mkString(" "))
    println("params")
    for ((k,v) <- path_params_map) println(s"key: $k, value: $v")


    //do a shallow copy of the url_chunks
    val url_chunks_tmp = chunks map(identity)


    val keys = Array("junk1", "var1", "var2", "junk2","junk3","var3")
    val vals = Array("junk1v", "var1v", "var2v", "junk2v","junk3v","var3v")

    val keysLength = keys.length
    var cnt = 0
    //val outArrB : ArrayBuffer[String] = new ArrayBuffer[String](keysLength)

    //TODO - before we get here, check if there are any path params; if not, just return the URL as is.

    //substitute in path parameters
    while (cnt < keysLength) {
      if (path_params_map.contains(keys(cnt))){
        val dex = path_params_map(keys(cnt))
        url_chunks_tmp(dex) = vals(cnt)
      }
      cnt += 1;
    }

    //join chunks_tmp into our new url
    val url_tmp = url_chunks_tmp.mkString("")

    println("resultant url is ", url_tmp)

    //lets take two arrays, on of keys, one of lists; given a map of key to array index, copy the values into the the shallow array
    //lets split apart a swagger url spec, creating an array of blocks, separated by {}'s; shove the value betweeen the {}s into the same array.
    //  keep a keys to indexes in template array



    /*
    val url = "http://hostname:port/some{var1}/some/{var2}/isGood?{var3}"

    //go through url, split around path params to get keys, sub in keys matching values.

    //I want a fast way to pop values into bits of a string.
    // it should work as though I have a into
    val
    for (c <- url) {

    }

      val keys: Array[String]
      val values: Array[String]

      val keysLength = keys.length
      var cnt = 0
      val outArrB : ArrayBuffer[String] = new ArrayBuffer[String](keysLength)

      while (cnt < keysLength) {
        outArrB += URLEncoder.encode(keys(cnt)) + "=" + URLEncoder.encode(values(cnt))
        cnt += 1
      }

      outArrB.mkString("&")
*/

  }
}

