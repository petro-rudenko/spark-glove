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
package nlp.glove

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2Vec
/**
 * Evaluates performance of this model
 * and compares accuracy with word2vec
 */
object Evaluate {

  def word2vec() = {

    val sc = new SparkContext("local", "Word2Vec")
    val input = sc.textFile("data/text8").map(line => line.split(" ").toSeq)
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("china", 40)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
  }

}
