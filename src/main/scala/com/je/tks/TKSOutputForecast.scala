package com.je.tks

import org.apache.spark.{SparkConf, SparkContext}

object TKSOutputForecast {
  def main(args: Array[String]):Unit = {
    print("Hello world 1")
    val sparkConf = new SparkConf().setAppName("ProductHourly_test_02")
    print("Hello world 2")
  }
}
