package com.example

import com.mapr.db.spark.sql.toSparkSessionFunctions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{explode, lit, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.store.DriverManager
import org.ojai.{Document, DocumentStream, Value}

import java.util.UUID
import scala.collection.JavaConverters._

object ConnectionObject {
  @transient lazy val connection = DriverManager.getConnection("ojai:mapr:")
  @transient lazy val store = connection.getStore("/tables/movie")
}

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

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Set("/apps/stream:read2"), kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy)

    //  optymalne - print executorami
//        messagesDStream.foreachRDD(batchRDD => {
//          batchRDD.foreachPartition {
//            iter => {
//              val elemsOnParition = iter.toList
//              elemsOnParition.map { elem =>
//                println(elem.value)
//              }
//            }
//          }
//        })

    //    nieoptymalne - print batchowo na driverze
    //    val valuesDStream = messagesDStream.map(_.value())
    //    valuesDStream.count
    //    valuesDStream.print

    val sadStream = messagesDStream.mapPartitions {
      iterator => {

        //TODO: connection i store utworzyc raz - doczytac o partycjach, poszukac jak utworzyc to raz per executor - done
        //TODO zrozumiec kod, gdzie sie jaka czesc wykonuje - done

      val list = iterator
        .map(record => record.value())
        .toList
        .asJava
        //TODO serializacja, deserializacja, interface serializable w javie - done
      val query = ConnectionObject.connection
        .newQuery()
        .where(ConnectionObject.connection.newCondition()
          .in("_id", list)
          .build())
        .build()

      val result: DocumentStream = ConnectionObject.store.findQuery(query)
      val res1 = result.asScala.toList.map(x => Movie(x.getString("_id"), x.getString("imdb_title_id"), x.getString("title"),
        x.getString("original_title"), tryGetInt(x, "year"), x.getString("date_published"),
        x.getString("genre"), tryGetInt(x, "duration"), tryGetString(x, "country_id"),
        tryGetString(x, "language"), tryGetString(x, "director"), tryGetString(x, "writer"),
        tryGetString(x, "production_company"), tryGetString(x, "actors"),
        tryGetString(x, "description"), x.getDouble("avg_vote"),
        x.getInt("votes"), tryGetString(x, "budget"), tryGetString(x, "usa_gross_income"),
        tryGetString(x, "worlwide_gross_income"), tryGetDouble(x, "metascore"),
        tryGetDouble(x, "reviews_from_users"), tryGetDouble(x, "reviews_from_critics"))).toIterator

//      connection.close()
//      store.close()

      res1
    }
    }

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val dictionaryDS = spark.loadFromMapRDB("/tables/country").as[CountryDictionary]
    dictionaryDS.show()

    sadStream.foreachRDD(batchRDD => {
      val futureDS = batchRDD
      val incompleteFullMovies = futureDS.toDS()
//      incompleteFullMovies.show(10)
//      dictionaryDS.show()

      //TODO w jaki sposob rozbic df na zmatchowane i niezmatchowane, sparkowo - done
      val joined: DataFrame = incompleteFullMovies.join(dictionaryDS, incompleteFullMovies("country_id") <=> dictionaryDS("_id"), "left_outer").drop("_id")

      val emptyMovies: DataFrame = joined.filter('country.isNull || 'country_id.isNull)
//      emptyMovies.saveToMapRDB("/tables/countrylessMovies")
      println("In this batch there are " + emptyMovies.count() + " empty movie records. Saved them to /tables/countrylessMovies.")
      val goodMovies = joined.filter('country.isNotNull && 'country_id.isNotNull)
//      goodMovies.show()
      val generateUUID = udf((a:Any) => UUID.randomUUID().toString)

      val withUUID = goodMovies.withColumn("_id",generateUUID($"country"))
//      withUUID.show()
//      withUUID.printSchema()
//      withUUID.saveToMapRDB("/tables/movie_enriched_with_country")

//      withUUID
//        .selectExpr("CAST(_id AS STRING) AS key", "to_json(struct(*)) AS value")
//        .write.format("kafka")
//        .option("topic","/apps/stream:index")
//        .save()

      val explodedDF = withUUID
        .withColumn("genre",functions.split($"genre",", "))
        .withColumn("genre",explode($"genre"))
//      explodedDF.show()

      val newDF = explodedDF.withColumn("_id", functions.concat(explodedDF("_id").cast(StringType),
        lit("_").cast(StringType),
        explodedDF("genre").cast(StringType)))
//      newDF.show()
//      newDF.saveToMapRDB("/tables/genre")
    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}

