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


object Scratch2 {


  def main(args: Array[String]): Unit = {


    val url = "http://hostname:port/some{var1}/some/{var2}/isGood?{var3}"

    val urlt = new URLTemplate(url)

    val keys = Array("junk1", "var1", "var2", "junk2", "junk3", "var3")
    val vals = Array("junk1v", "var1v", "var2v", "junk2v", "junk3v", "var3v")

    val url_tmp = urlt.hydrate(keys, vals)
    println("resultant url is " + url_tmp)

  }
}