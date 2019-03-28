package hk.hku.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import hk.hku.spark.corenlp.CoreNLPSentimentAnalyzer
import hk.hku.spark.mllib.MLlibSentimentAnalyzer
import hk.hku.spark.utils._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import shaded.parquet.org.apache.thrift.Option.Some
import twitter4j.{Status, TwitterException, TwitterObjectFactory}
import twitter4j.auth.OAuthAuthorization

/**
  * Analyzes and predicts Twitter Sentiment in [near] real-time using Spark Streaming and Spark MLlib.
  * Uses the Naive Bayes Model created from the Training data and applies it to predict the sentiment of tweets
  * collected in real-time with Spark Streaming, whose batch is set to 20 seconds [configurable].
  * Raw tweets [compressed] and also the gist of predicted tweets are saved to the disk.
  * At the end of the batch, the gist of predicted tweets is published to Redis.
  * Any frontend app can subscribe to this Redis Channel for data visualization.
  */
// spark-submit --class "hk.hku.spark.TweetSentimentAnalyzer" --master local[3] /opt/spark-twitter/7305CloudProject/StreamProcessorSpark/target/StreamProcessorSpark-jar-with-dependencies.jar
object TweetSentimentAnalyzer {

  @transient
  lazy val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //    val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)
    val ssc = createSparkStreamingContext
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    log.info("TweetSentimentAnalyzer start ")

    // 广播 Kafka Producer 到每一个excutors
    val kafkaProducer: Broadcast[BroadcastKafkaProducer[String, String]] = {
      val kafkaProducerConfig = {
        val prop = new Properties()
        prop.put("group.id", PropertiesLoader.groupIdProducer)
        prop.put("acks", "all")
        prop.put("retries ", "1")
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesLoader.bootstrapServersProducer)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        prop
      }
      ssc.sparkContext.broadcast(BroadcastKafkaProducer[String, String](kafkaProducerConfig))
    }

    // Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopWordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

    /**
      * Predicts the sentiment of the tweet passed.
      * Invokes Stanford Core NLP and MLlib methods for identifying the tweet sentiment.
      *
      * @param status -- twitter4j.Status object.
      * @return tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity, Latitude, Longitude,
      *         Profile Image URL, Tweet Date.
      */
    def predictSentiment(status: Status): (Long, String, String, Int, Int, Double, Double, String, String) = {
      val tweetText = replaceNewLines(status.getText)

      log.info("tweetText : " + tweetText)

      val (corenlpSentiment, mllibSentiment) =
        (0, MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel))
      //      CoreNLPSentimentAnalyzer.computeWeightedSentiment(tweetText),

      if (hasGeoLocation(status))
        (status.getId,
          status.getUser.getScreenName,
          tweetText,
          corenlpSentiment,
          mllibSentiment,
          status.getGeoLocation.getLatitude,
          status.getGeoLocation.getLongitude,
          status.getUser.getOriginalProfileImageURL,
          simpleDateFormat.format(status.getCreatedAt))
      else
        (status.getId,
          status.getUser.getScreenName,
          tweetText,
          corenlpSentiment,
          mllibSentiment,
          -1,
          -1,
          status.getUser.getOriginalProfileImageURL,
          simpleDateFormat.format(status.getCreatedAt))
    }


    // 直接读取Twitter API 数据
    //    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    //    val rawTweets = TwitterUtils.createStream(ssc, oAuth)


    // kafka consumer 参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesLoader.bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> PropertiesLoader.groupId,
      "auto.offset.reset" -> PropertiesLoader.autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // topics
    val topics = PropertiesLoader.topics.split(",").toSet

    // 读取 Kafka 数据---json 格式字符串
    val rawTweets = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // 保存Tweet 元数据
    //    if (PropertiesLoader.saveRawTweets) {
    //      rawTweets.cache()
    //      rawTweets.foreachRDD { rdd =>
    //        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
    //          saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
    //        }
    //      }
    //    }

    val classifiedTweets = rawTweets
      .filter(line => !line.value().isEmpty)
      .map(line => {
        // kafka key值是null
        log.info("message key : " + line.key())
        log.info("message value : " + line.value())
        log.info("message str : " + line.value().toString)
        TwitterObjectFactory.createStatus(line.value())
      })
      .filter({
        case null =>
          false
        case _ =>
          true
      }
        //        status => {
        //          if (status == null || !isTweetInEnglish(status)) {
        //            log.info(s"tweets filter data : ${status.getId}")
        //            false
        //          }
        //          else {
        //            log.info(s"tweets : ${status.getId}")
        //            true
        //          }
        //        }
      )
      .map(predictSentiment)

    // 分隔符 was chosen as the probability of this character appearing in tweets is very less.
    val DELIMITER = "¦"
    //    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath

    classifiedTweets.foreachRDD { rdd =>
      try {
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          // saveClassifiedTweets(rdd, tweetsClassifiedPath)

          // produce message to kafka
          rdd.foreach(message => {
            log.info(s"producer msg to kafka ${message.toString()}")
            // id, screenName, text, sent1, sent2, lat, long, profileURL, date
            kafkaProducer.value.send(PropertiesLoader.topicProducer, message.productIterator.mkString(DELIMITER))
          })
        } else {
          log.warn("classifiedTweets rdd is null")
        }
      } catch {
        case e: TwitterException =>
          log.error("classifiedTweets TwitterException")
          log.error(e)
        case e: Exception =>
          log.error(e)
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) // auto-kill after processing rawTweets for n mins.
  }

  /**
    * Create StreamingContext.
    * Future extension: enable checkpointing to HDFS [is it really required??].
    *
    * @return StreamingContext
    */
  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // For reconstructing the Web UI after the application has finished.
      .set("spark.eventLog.enabled", "true")
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }

  /**
    * Saves the classified tweets to the csv file.
    * Uses DataFrames to accomplish this task.
    *
    * @param rdd                  tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity,
    *                             Latitude, Longitude, Profile Image URL, Tweet Date.
    * @param tweetsClassifiedPath Location of saving the data.
    */
  def saveClassifiedTweets(rdd: RDD[(Long, String, String, Int, Int, Double, Double, String, String)], tweetsClassifiedPath: String) = {
    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

    import sqlContext.implicits._

    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "CoreNLP", "MLlib", "Latitude", "Longitude", "ProfileURL", "Date")
    classifiedTweetsDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      // Will it be good if DF is partitioned by Sentiment value? Probably does not make sense.
      //.partitionBy("Sentiment")
      // TODO :: Bug in spark-csv package :: Append Mode does not work for CSV: https://github.com/databricks/spark-csv/issues/122
      //.mode(SaveMode.Append)
      .save(tweetsClassifiedPath + now)
  }

  /**
    * Jackson Object Mapper for mapping twitter4j.Status object to a String for saving raw tweet.
    */
  val jacksonObjectMapper: ObjectMapper = new ObjectMapper()

  /**
    * Saves raw tweets received from Twitter Streaming API in
    *
    * @param rdd           -- RDD of Status objects to save.
    * @param tweetsRawPath -- Path of the folder where raw tweets are saved.
    */
  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    val rawTweetsDF = sqlContext.read.json(tweet)
    rawTweetsDF.coalesce(1).write
      .format("org.apache.spark.sql.json")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  /**
    * Removes all new lines from the text passed.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String without new lines.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Checks if the tweet Status is in English language.
    * Actually uses profile's language as well as the Twitter ML predicted language to be sure that this tweet is
    * indeed English.
    *
    * @param status twitter4j Status object
    * @return Boolean status of tweet in English or not.
    */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  /**
    * Checks if the tweet Status has Geo-Coordinates.
    *
    * @param status twitter4j Status object
    * @return Boolean status of presence of Geolocation of the tweet.
    */
  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}
