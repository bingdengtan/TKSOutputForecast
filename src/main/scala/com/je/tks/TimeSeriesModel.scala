package com.je.tks



import java.text.SimpleDateFormat
import java.util.Calendar

import com.cloudera.sparkts.TimeSeriesRDD
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


class TimeSeriesModel{
  private var predictedN = 1
  private var outputTableName = "timeseries_output"

  def this(predictedN:Int, outputTableName:String){
    this()
    this.predictedN = predictedN
    this.outputTableName = outputTableName
  }

  /**
    * Arima模型：
    * 输出其p，d，q参数
    * 输出其预测的predictedN个值
    * @param trainTsrdd
    */
  def arimaModelTrain(trainTsrdd: TimeSeriesRDD[String], listPDQ: List[String]): (RDD[(String, Vector)], RDD[(String, (String, (String, String, String), String, String))]) =  {
    val predictedN = this.predictedN

    /** *创建arima模型 ***/
    //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
    val arimaAndVectorRdd = trainTsrdd.map { line =>
      line match {
        case (key, denseVector) => {
          if (listPDQ.size >= 3) {
            (key, ARIMA.fitModel(listPDQ(0).toInt, listPDQ(1).toInt, listPDQ(2).toInt, denseVector), denseVector) }
          else {
            (key, ARIMA.autoFit(denseVector), denseVector)
          }
        }
      }
    }

    /** 参数输出:p,d,q的实际值和其系数值、最大似然估计值、aic值 **/
    val coefficients = arimaAndVectorRdd.map { line =>
      line match {
        case (key, arimaModel, denseVector) => {
          (key, (arimaModel.coefficients.mkString(","),
            (arimaModel.p.toString,
              arimaModel.d.toString,
              arimaModel.q.toString),
            arimaModel.logLikelihoodCSS(denseVector).toString,
            arimaModel.approxAIC(denseVector).toString))
        }
      }
    }

    coefficients.collect().map {
      _ match {
        case (key, (coefficients, (p, d, q), logLikelihood, aic)) =>
          println(key + " coefficients:" + coefficients + "=>" + "(p=" + p + ",d=" + d + ",q=" + q + ")")
      }
    }

    /***预测出后N个的值*****/
    val forecast = arimaAndVectorRdd.map{row=>
      row match{
        case (key, arimaModel, denseVector) => {
          (key, arimaModel.forecast(denseVector, predictedN))
        }
      }
    }

    //取出预测值
    val forecastValue = forecast.map{
      _ match {
        case (key, value) => {
          val partArray = value.toArray.mkString(",").split(",")
          var forecastArrayBuffer = new ArrayBuffer[Double]()
          var i = partArray.length - predictedN
          while (i < partArray.length) {
            forecastArrayBuffer += partArray(i).toDouble
            i = i + 1
          }
          (key, Vectors.dense(forecastArrayBuffer.toArray))
        }
      }
    }

    println("Arima forecast of next " + predictedN + " observations:")
    forecastValue.foreach(println)
    return (forecastValue, coefficients)
  }



  /**
    * Arima模型评估参数的保存
    * coefficients、（p、d、q）、logLikelihoodCSS、Aic、mean、variance、standard_deviation、max、min、range、count *
    * @param sparkSession
    * @param coefficients
    * @param forecastValue
    */
  def arimaModelKeyEvaluationSave(sparkSession: SparkSession, coefficients: RDD[(String, (String, (String, String, String), String, String))], forecastValue: RDD[(String, Vector)]): Unit = {
    /** 把vector转置 **/
    val forecastRdd = forecastValue.map {
      _ match {
        case (key, forecast) => forecast.toArray
      }
    }

    // Split the matrix into one number per line.
    val byColumnAndRow = forecastRdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    val summary = Statistics.colStats(transposed.map(value => Vectors.dense(value(0))))

    /** 统计求出预测值的均值、方差、标准差、最大值、最小值、极差、数量等;合并模型评估数据+统计值 **/
    //评估模型的参数+预测出来数据的统计值
    val evaluation = coefficients.join(forecastValue.map {
      _ match  {
        case (key, forecast) => {
          (key, (summary.mean.toArray(0).toString,
            summary.variance.toArray(0).toString,
            math.sqrt(summary.variance.toArray(0)).toString,
            summary.max.toArray(0).toString,
            summary.min.toArray(0).toString,
            (summary.max.toArray(0) - summary.min.toArray(0)).toString,
            summary.count.toString))
        }
      }
    })

    val evaluationRddRow = evaluation.map {
      _ match {
        case (key, ((coefficients, pdq, logLikelihoodCSS, aic), (mean, variance, standardDeviation, max, min, range, count))) => {
          Row(coefficients, pdq.toString, logLikelihoodCSS, aic, mean, variance, standardDeviation, max, min, range, count)
        }
      }
    }

    //形成评估dataframe
    val schemaString = "coefficients,pdq,logLikelihoodCSS,aic,mean,variance,standardDeviation,max,min,range,count"
    val schema = StructType(schemaString.split(",").map(fileName => StructField(fileName, StringType, true)))
    val evaluationDf = sparkSession.createDataFrame(evaluationRddRow, schema)
    println("Evaluation in Arima:")
    evaluationDf.show()

    /** * 把这份数据保存到hive与db中 */
    evaluationDf.write.mode(SaveMode.Overwrite).saveAsTable(outputTableName + "_arima_evaluation")
  }
}
