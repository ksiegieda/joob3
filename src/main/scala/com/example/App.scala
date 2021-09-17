package com.example

import com.mapr.db.spark.sql.toSparkSessionFunctions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.store.DriverManager
import org.ojai.{Document, DocumentStream, Value}

import java.util.UUID
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
          val incompleteFullMovie: List[FullMovie] = movieList.map(_.as[FullMovie])
          incompleteFullMovie.toIterator
          //TODO: kompletnie niepotrzebne mapowanie Movie na FullMovie, zostawic jako zwykly Movie, potem jest burdel w joinie
        }
      }
      val spark = SparkSession.builder.config(futureDS.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val incompleteFullMovies = futureDS.toDS()
//      incompleteFullMovies.show(10)

      val dictionaryDS = spark.loadFromMapRDB("/tables/country").as[Dictionary]
//      dictionaryDS.show()

      val dropping: DataFrame = incompleteFullMovies.drop("country")
      val joined: DataFrame = dropping.join(dictionaryDS, dropping("country_id") <=> dictionaryDS("_id"), "fullouter").drop("_id")
//      joined.show()

      //TODO: join wyglada zle, ale dlatego, ze slownik zawiera wszystkie kraje, natomiast stream z Joba 2 ograniczono do 20 pierwszych IDkow - trzeba wysylac wszystkie
      val generateUUID = udf((a:String) => UUID.nameUUIDFromBytes(a.getBytes).toString)
      val withUUID = joined.withColumn("_id",generateUUID($"country"))
//      withUUID.show()
//      withUUID.saveToMapRDB("tables/movie_enriched_with_country")

      //TODO: tu mozna uzyc Kafka Serializera, Mateusz wysylal po pierwszym CR
      withUUID
        .selectExpr("CAST(_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .write.format("kafka")
        .option("topic","/apps/stream:index")
        .save()

      //TODO: dalej explode - mozna wziac z pierwszego joba

    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
    //TODO: jakis refactor by sie przydal, ale to na koniec koniec
}