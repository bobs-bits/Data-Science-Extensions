package org.apache.dsext.spark.datasource.rest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.internal.Logging
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

import java.io.Closeable
import java.io.Serializable
import org.apache.spark.internal.Logging

class URLTemplate(url : String)  extends Logging  {

  //are going tp split the url into chunks based on {}'s
  var chunks = ArrayBuffer[String]()

  //and we need to keep a map of where, among the chunks, are the path params
  val path_params_map = collection.mutable.Map[String, Int]()


  //chunk the url
  var buff = ArrayBuffer[Char]()
  var inBraces : Boolean = false
  for (c <- url) {
    logTrace(s"procssing character: " + c)
    if(c == '{') {
      if (buff.length > 0) {
        val chunk = buff.mkString("")
        logDebug(s"adding url chunk " + chunk)
        chunks += chunk
        buff.clear
      }
      inBraces = true;
    }
    else if(c == '}' ) {
      inBraces = false;
      val chunk = buff.mkString("")
      logDebug(s"adding url chunk " + chunk)
      chunks += chunk
      val dex = chunks.length - 1
      logDebug(s"adding path_param " + chunk + " at " + dex)
      path_params_map +=  ( chunk ->  dex )
      buff.clear()
    }
    else {
      buff += c;
    }
  }

  if(inBraces == true){
    throw new RuntimeException("unmatched left curly brace in url:" + url);
  }

  //add any left over buff
  if(buff.length > 0){
    val chunk = buff.mkString("")
    logDebug(s"adding chunk " + chunk)
    chunks += chunk
  }

  def hasPathParams() : Boolean = {
    if( path_params_map.size < 1) return false;
    return true;
  }

  def hydrate(keys: Array[String], values: Array[String]) : String = {

    if (!hasPathParams()){
      return url;

    }

    //do a shallow copy of the url_chunks
    val chunks_tmp = chunks map (identity)

    val keysLength = keys.length
    var cnt = 0

    //substitute in path parameters
    while (cnt < keysLength) {
      if (path_params_map.contains(keys(cnt))) {
        val dex = path_params_map(keys(cnt))
        chunks_tmp(dex) = values(cnt)
      }
      cnt += 1;
    }

    //TODO - check that each and every path param was resolved

    //join chunks_tmp into our new url
    val url_tmp = chunks_tmp.mkString("")
    return url_tmp;
  }
}



