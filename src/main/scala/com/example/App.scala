package com.example

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ojai.store.DriverManager
import org.ojai.{Document, DocumentStream, Value}

import scala.collection.JavaConverters._

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

    Logger.getLogger("org").setLevel(Level.ERROR)

    val groupId = "testgroup"
    val offsetReset = "earliest"
    val pollTimeout = "5000"

//    val sparkConf = new SparkConf().setAppName(App.getClass.getName)
//    val sc = new SparkContext(sparkConf)
//    val ssc = new StreamingContext(sc, Seconds(2))

    val spark = SparkSession
      .builder
      .appName("lat-job-3")
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

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

      val result: DocumentStream = store.findQuery(query)
      val res1 = result.asScala.toList.map(x => Movie(x.getString("_id"), x.getString("imdb_title_id"), x.getString("title"),
        x.getString("original_title"), tryGetInt(x, "year"), x.getString("date_published"),
        x.getString("genre"), tryGetInt(x, "duration"), tryGetString(x, "country_id"),
        tryGetString(x, "language"), tryGetString(x, "director"), tryGetString(x, "writer"),
        tryGetString(x, "production_company"), tryGetString(x, "actors"),
        tryGetString(x, "description"), x.getDouble("avg_vote"),
        x.getInt("votes"), tryGetString(x, "budget"), tryGetString(x, "usa_gross_income"),
        tryGetString(x, "worlwide_gross_income"), tryGetDouble(x, "metascore"),
        tryGetDouble(x, "reviews_from_users"), tryGetDouble(x, "reviews_from_critics"))).toIterator

      connection.close()
      store.close()

      res1
    }
    )

    sadStream.foreachRDD(batchRDD => {
      val futureDS = batchRDD.mapPartitions {
        iterator => {
          val movieList = iterator.toList
          val stringList = movieList.flatMap {
            x => x.country_id
              .getOrElse("")
              .split(", ")}
            .filterNot(_.isEmpty)
            .distinct

          val connection = DriverManager.getConnection("ojai:mapr:")
          val store = connection.getStore("/tables/country")
          val query = connection
            .newQuery()
            .where(connection.newCondition()
              .in("_id", stringList.asJava)
              .build())
            .build()

          val result: DocumentStream = store.findQuery(query)
          val resMap = result
            .asScala
            .toList
            .map(x => (x.getString("_id"), x.getString("country"))).toMap

//          spark.sparkContext.parallelize()
          val spark = SparkSession.builder.config(batchRDD.sparkContext.getConf).getOrCreate()
          val incompleteFullMovie: List[FullMovie] = movieList.map(_.as[FullMovie])
          //parallelize to RDD -> to DS
          import spark.implicits._
          val mama = incompleteFullMovie.toDS()
//          mama.show()
          val FullMovieList = incompleteFullMovie.map(
            movie => movie.copy(
              country = movie.country_id match {
                case Some(x) => Some(x.split(", ").map(resMap(_)).mkString(","))
                case None => None
              }
            )
          )
          val withUUID = FullMovieList.map((movie => movie.copy(_id = java.util.UUID.randomUUID().toString)))
          withUUID.toIterator
        }
      }
//      val spark = SparkSession.builder.config(futureDS.sparkContext.getConf).getOrCreate()
//      import spark.implicits._
//      val ds = futureDS.toDS()
//      ds.saveToMapRDB("tables/movie_enriched_with_country")
//      ds.show()
    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
//    TODO: add methods
  }

}