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

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import breeze.linalg.Vector
import breeze.linalg.functions.cosineDistance
import breeze.linalg.shuffle
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

/**
 * GloVe is an unsupervised learning algorithm for obtaining vector representations for words.
 * Training is performed on aggregated global word-word co-occurrence statistics from a corpus,
 * and the resulting representations showcase interesting linear substructures
 * of the word vector space.
 * Original C implementation, and research paper http://www-nlp.stanford.edu/projects/glove/
 * @param window number of context words from [-window, window]
 * @param numComponents number of latent dimensions
 * @param minCount minimum frequency to consider a vocabulary word
 * @param learningRate learning rate for SGD estimation.
 * @param alpha, maxCount weighting function parameters, not extremely sensitive to corpus,
 *        though may need adjustment for very small or very large corpora
 */
class Glove(
    window:        Int    = 5,
    numComponents: Int    = 50,
    minCount:      Int    = 5,
    learningRate:  Double = 0.05,
    alpha:         Double = 0.75,
    maxCount:      Double = 100.0
) extends Serializable with Logging {

  var vocabHash = Map.empty[String, Int]
  var wordVec: DenseMatrix[Double] = _

  var vocabSize: Long = 0

  private def coocurMatrix(words: RDD[String]): Map[(Int, Int), Double] = {
    val vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .collectAsMap()

    vocabSize = vocab.keys.foldLeft(0) { case (i, w) => { vocabHash += w -> i; i + 1 } }
    logInfo("trainWordsCount = " + vocabSize)

    val sc = words.context
    val bcVocabHash = sc.broadcast(vocabHash)

    val coocurrenceMatrix: RDD[(Int, Int, Double)] = words.mapPartitionsWithIndex { (pnum, iter) =>
      {
        var coocurences = scala.collection.mutable.HashMap.empty[(Int, Int), Double]
        var windowBuffer = List.empty[Int] // TODO: Use ring buffer instead.
        iter.foreach { w =>
          val word = bcVocabHash.value.get(w).map((w) => {
            for {
              (contextWord, i) <- windowBuffer.zipWithIndex
              if (w != contextWord)
              w1 = Math.min(w, contextWord)
              w2 = Math.max(w, contextWord)
            } {
              coocurences += (w1, w2) -> (coocurences.getOrElse((w1, w2), 0.0) + 1.0 / (i + 1))
            }
            windowBuffer ::= w
            if (windowBuffer.size == window) windowBuffer = windowBuffer.init
          })
        }
        logInfo("founded " + coocurences.size + " coocurences on partition " + pnum)
        coocurences.map { case (k, v) => (k._1, k._2, v) }.toSeq.iterator
      }
    }

    coocurrenceMatrix
      .groupBy(s => s._1 -> s._2)
      .flatMapValues(x => x.map(_._3))
      .reduceByKey((w1, w2) => w1 + w2)
      .collect().toMap //TODO: keep coocurence map distributed 
  }

  def fit(sentences: RDD[Seq[String]], epochs: Int = 5) = {
    val coocMatrix = coocurMatrix(sentences.flatMap(w => w)) // TODO: distributed train
    val nnzEntries = coocMatrix.keySet.toIndexedSeq
    val cols = vocabSize.toInt

    wordVec = DenseMatrix.rand(cols, numComponents)
    val wordBiases = DenseVector.zeros[Double](cols)
    val shuffleIndices = DenseVector.range(0, nnzEntries.size)
    (0 to epochs).foreach(_ => {
      val shuffled = shuffle(shuffleIndices)
      shuffled.foreach(j => {
        val (w1, w2) = nnzEntries(j)
        val count = coocMatrix((w1, w2))
        var prediction = 0.0
        var word_a_norm = 0.0
        var word_b_norm = 0.0
        for (i <- 0 until wordVec.cols) {
          val w1Context = wordVec(w1, i)
          val w2Context = wordVec(w2, i)
          prediction = prediction + w1Context + w2Context
          word_a_norm += w1Context * w1Context
          word_b_norm += w2Context * w2Context
        }
        prediction = prediction + wordBiases(w1) + wordBiases(w2)
        word_a_norm = Math.sqrt(word_a_norm)
        word_b_norm = Math.sqrt(word_b_norm)
        val entryWeight = Math.pow(Math.min(1.0, (count / maxCount)), alpha)
        val loss = entryWeight * (prediction - Math.log(count))
        for (i <- 0 until wordVec.cols) {
          wordVec(w1, i) = (wordVec(w1, i) - learningRate * loss * wordVec(w2, i)) / word_a_norm
          wordVec(w2, i) = (wordVec(w2, i) - learningRate * loss * wordVec(w2, i)) / word_b_norm
        }
        wordBiases(w1) -= learningRate * loss
        wordBiases(w2) -= learningRate * loss
      })
    })

  }

  /**
   * Transforms a word to its vector representation
   * @param word a word
   * @return vector representation of word
   */
  def transform(word: String): Vector[Double] = {
    vocabHash.get(word) match {
      case Some(v) =>
        wordVec(v, ::).t
      case None =>
        throw new IllegalStateException(s"$word not in vocabulary")
    }
  }

  /**
   * Find synonyms of a word
   * @param word a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word)
    findSynonyms(vector, num)
  }

  /**
   * Find synonyms of the vector representation of a word
   * @param vector vector representation of a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
   */
  def findSynonyms(vector: Vector[Double], num: Int): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    // TODO: optimize top-k
    (vocabHash).map { case (w, i) => (w, cosineDistance(vector, wordVec(i, ::).t)) }
      .toSeq
      .sortBy(-_._2)
      .take(num + 1)
      .tail
      .toArray
  }

  /**
   * Find n analogies that relate to target word as w1 relates to w2.
   */
  def analogyWords(w1: String, w2: String, target: String, n: Int, accuracy: Double = 0.0001): Array[(String, Double)] = {
    val distance = cosineDistance(transform(w1), transform(w2))
    val targetVec = transform(target)
    (vocabHash).map { case (w, i) => (w, cosineDistance(targetVec, wordVec(i, ::).t)) }
      .filter(d => Math.abs(d._2) < accuracy)
      .toSeq
      .sortBy(-_._2)
      .take(n + 1)
      .toArray
  }

  def load = ???
  def save = ???

}

object Glove {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Glove2")
    val input = Corpus.text8(sc)
    val glove = new Glove(window = 3)
    glove.fit(input)
    val synonyms = glove.findSynonyms("china", 10)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
    sc.stop()
  }

}
