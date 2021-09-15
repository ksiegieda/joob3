package com.example

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark._
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ojai.store.DriverManager
import org.ojai.{Document, DocumentStream, Value}

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

  object App {

    def tryGetInt(x:Document, field:String): Option[Int] = x.getValue(field).getType.getCode match {
      case none if none == Value.Type.NULL.getCode => None
      case int if int == Value.Type.INT.getCode => Some(x.getInt(field))
    }
    def tryGetString(x:Document, field:String): Option[String] = x.getValue(field).getType.getCode match {
      case none if none == Value.Type.NULL.getCode => None
      case str if str == Value.Type.STRING.getCode => Some(x.getString(field))
    }
    def tryGetDouble(x:Document, field:String): Option[Double] = x.getValue(field).getType.getCode match {
      case none if none == Value.Type.NULL.getCode => None
      case double if double == Value.Type.DOUBLE.getCode => Some(x.getDouble(field))
    }

  def main(args: Array[String]) = {

    val groupId = "testgroup"
    val offsetReset = "earliest"
    val pollTimeout = "5000"

    val sparkConf = new SparkConf().setAppName(App.getClass.getName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaParams = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Set("/apps/stream:read"), kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy)

//  optymalne - print executorami
//    messagesDStream.foreachRDD(batchRDD => {
//      batchRDD.foreachPartition {
//        iter => {
//          val elemsOnParition = iter.toList
//          elemsOnParition.map { elem =>
//            println(elem.value)
//          }
//        }
//      }
//    })

//    nieoptymalne - print batchowo na driverze
//    val valuesDStream = messagesDStream.map(_.value())
//    valuesDStream.count
//    valuesDStream.print

    val sadStream = messagesDStream.mapPartitions (iterator => {
      val connection = DriverManager.getConnection("ojai:mapr:")
      val store = connection.getStore("/tables/movie")

      val list = iterator
        .map(record => record.value())
        .toList
        .asJava
      val query = connection
        .newQuery()
        .where(connection.newCondition()
            .in("_id", list)
            .build())
        .build()

      //TODO: findQuery is deprecated
      val result: DocumentStream = store.findQuery(query)
        result.asScala.toList.map(x => Movie(x.getString("_id"), x.getString("imdb_title_id"), x.getString("title"),
        x.getString("original_title"), tryGetInt(x, "year"), x.getString("date_published"),
        x.getString("genre"), tryGetInt(x, "duration"), tryGetString(x, "country_id"),
        tryGetString(x, "language"), tryGetString(x, "director"), tryGetString(x, "writer"),
        tryGetString(x, "production_company"), tryGetString(x, "actors"),
        tryGetString(x, "description"), x.getDouble("avg_vote"),
        x.getInt("votes"), tryGetString(x, "budget"), tryGetString(x, "usa_gross_income"),
        tryGetString(x, "worlwide_gross_income"), tryGetDouble(x, "metascore"),
        tryGetDouble(x, "reviews_from_users"), tryGetDouble(x, "reviews_from_critics"))).toIterator

//TODO: close the store and OJAI connection
    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
//    TODO: add methods
  }

}