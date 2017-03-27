package etl
//import org.apache.log4j.{Level, Logger}
import play.api.libs.json._
import scala.util.parsing.json.JSON._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
//


//**********************************************************
//**********************************************************
//**********************************************************
//Declaring case classes for different types of data
//**********************************************************
//**********************************************************
//**********************************************************

case class Verbiage(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                    model: String, io: String, prompt: String,
                    channelMatch: String, languageOutput: String)

case class Conversation(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                        model: String, io: String, utterance: String, responseCode: String, response: String,
                        toneEmotion: Option[String], toneScore: Option[Double],
                        timeOfDay: Option[String], channel: Option[String], intent: String,
                        entities: Option[Map[String, String]], confidenceScore: Double)

case class Dialog(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                  model: String, seqNo: Int, utterance: Option[String], translation: Option[String],
                  responseType: String, prompt: String)

case class Context(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                   model: String, contextAction: String, io: String, inputStatus: String,
                   inputMethod: String, custState: Option[String], channel: Option[String],
                   timeOfDay: Option[String], iiDigits: Option[String], channelChange: Option[String],
                   toneEmotion: Option[String], turnCount: Option[String], nmCounter: Option[String],
                   custServAddress: Option[String], toneScore: Option[String], firstTurn: Option[String])

/**
  * Created by jswortz on 3/15/2017.
  */

  /**
    * Created by jswortz on 3/13/2017.
    */
  object etl {

    def main(args: Array[String]): Unit = {
    //.getLogger("org").setLevel(Level.WARN)

    // When running spark app, indicate HDFS input path for analysis as the first argument

    val path = {
      if (args.length > 0)
        args(0)
      else
        "assets123" // replace with runtime parameters
    }

    val conf = new SparkConf().setAppName("POC ETL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //val sc = spark.sparkContext

    val data = sc.textFile(path).map(_.split('|'))

    //**********************************************************
    //**********************************************************
    //**********************************************************
    //helper function to get the value in a "KEY=VALUE" string
    //**********************************************************
    //**********************************************************
    //**********************************************************

//    def getValOnly(input: String): String = {
//      input.split("=") match {
//        case Array(x) => x
//        case Array(x, y) => y
//        case _ => "error"
//      }
//    }

    def epochConverter(epochString: String): Double = epochString.split(" ") match {
      case Array(a, b) => a.toDouble + b.toDouble
      case Array(a) => a.toDouble
      case _ => 0.0
    }

    def jsonParser(input: String): play.api.libs.json.JsValue = {
      if (parseFull(input).isDefined) Json.parse(input)
      else Json.parse("{}")
    }


    def entityMapper(parssed: play.api.libs.json.JsValue): Option[Map[String, String]] = Option(parssed) match {
      case Some(x) => Option(((x \ "entities" \\ "entity").map(_.toString) zip
        (x \ "entities" \\ "value").map(_.toString)).toMap)
      case _ => None
    }

    //  def json2map(input: String): Map[String, Object] = {
    //    val mapper = new ObjectMapper() with ScalaObjectMapper
    //    mapper.registerModule(DefaultScalaModule)
    //    mapper.readValue[Map[String, Object]](input).withDefaultValue("NA")
    //  }
    //


    // cant exceed 22 columns!


    //**********************************************************
    //**********************************************************
    //**********************************************************
    //Matching where input data should utilize case classes
    //**********************************************************
    //**********************************************************
    //**********************************************************

    def verb(line: Array[String]): Option[Verbiage] = line match {
      case Array("Voice", u1, u2, a, b, x, c, q, e, f, g) => new Some(Verbiage(u1, u2,
        Option(a.toInt), Option(epochConverter(b)),
        x, c, q, e, f, g))
      case _ => None
    }

    def conversation(line: Array[String]): Option[Conversation] = line match {
      case Array("Conversation", u1, u2, a, b, x, c, "OUTPUT", e, f, g) => new Some(Conversation(u1, u2,
        Option(a.toInt), Option(epochConverter(b)),
        x, c, "OUTPUT", e, f, "", Option(""), Option(0.0), Option(""), Option(""), (jsonParser(g) \ "intents" \\ "intent")
          .headOption.getOrElse("").toString, entityMapper(jsonParser(g)),
        (jsonParser(g) \ "intents" \\ "confidence").headOption.getOrElse(0.0).toString.toDouble))
      case Array("Conversation", u1, u2, a, b, x, c, "INPUT", e, f) => new Some(Conversation(u1, u2,
        Option(a.toInt), Option(epochConverter(b)),
        x, c, "INPUT", "", "", e, (jsonParser(f) \ "context" \ "toneEmotion").asOpt[String],
        (jsonParser(f) \ "context" \ "toneScore").asOpt[Double],
        (jsonParser(f) \ "context" \ "timeOfDay").asOpt[String], (jsonParser(f) \ "context" \ "channel").asOpt[String]
        , "", None, 0.0))
      case _ => None
    }

    def dialog(line: Array[String]): Option[Dialog] = line match {
      case Array("Dialog", u1, u2, a, b, x, c, y, e, f, g) => new Some(Dialog(u1, u2, Option(a.toInt),
        Option(epochConverter(b)),
        x, c, y.toInt, if (e.split(':').length > 0) Option(e.split(':')(0)) else Option(""),
        if (e.split(':').length > 1) Option(e.split(':')(1)) else Option(""), f, g))
      case _ => None
    }


    def context2(line: Array[String]): Option[Context] = line match {
      case Array("ContextService", u1, u2, a, b, x, c, d, e, f) => new Some(Context(u1, u2, Option(a.toInt),
        Option(epochConverter(b)),
        x, c, "", e, "", "", (jsonParser(f) \ "custState").asOpt[String], (jsonParser(f) \ "channel").asOpt[String],
        (jsonParser(f) \ "timeOfDay").asOpt[String], (jsonParser(f) \ "iiDigits").asOpt[String], (jsonParser(f) \ "channelChange").asOpt[String],
        (jsonParser(f) \ "toneEmotion").asOpt[String], (jsonParser(f) \ "turnCount").asOpt[String], (jsonParser(f) \ "nmcounter").asOpt[String],
        (jsonParser(f) \ "custServiceAddress").asOpt[String], (jsonParser(f) \ "toneScore").asOpt[String], (jsonParser(f) \ "firstTurn").asOpt[String]))
      case Array("ContextService", u1, u2, a, b, x, c, d, e, f, g) => new Some(Context(u1, u2, Option(a.toInt),
        Option(epochConverter(b)),
        x, c, d, e, f, g, None, None, None, None, None, None, None, None, None, None, None))
      case Array("ContextService", u1, u2, a, b, x, c, d, e, f, g, h) => new Some(Context(u1, u2, Option(a.toInt),
        Option(epochConverter(b)),
        x, c, d, e, f, g, (jsonParser(h) \ "custState").asOpt[String], (jsonParser(h) \ "channel").asOpt[String],
        (jsonParser(h) \ "timeOfDay").asOpt[String], (jsonParser(h) \ "iiDigits").asOpt[String], (jsonParser(h) \ "channelChange").asOpt[String],
        (jsonParser(h) \ "toneEmotion").asOpt[String], (jsonParser(h) \ "turnCount").asOpt[String], (jsonParser(h) \ "nmcounter").asOpt[String],
        (jsonParser(h) \ "custServiceAddress").asOpt[String], (jsonParser(h) \ "toneScore").asOpt[String], (jsonParser(h) \ "firstTurn").asOpt[String]))
      // can't handle more than 22 fields :(
      case _ => None
    }


    //**********************************************************
    //**********************************************************
    //**********************************************************
    //creating final dataframes from case-class homogenous datasets
    //**********************************************************
    //**********************************************************
    //**********************************************************

    val verbOnly = data.map(verb).filter(_.isDefined).map(_.get)

    val verbDF = sqlContext.createDataFrame(verbOnly)

    val conversationOnly = data.map(conversation).filter(_.isDefined).map(_.get)

    val conversationDF = sqlContext.createDataFrame(conversationOnly)

    val dialogOnly = data.map(dialog).filter(_.isDefined).map(_.get)

    val dialogDF = sqlContext.createDataFrame(dialogOnly)

    val contextOnly = data.map(context2).filter(_.isDefined).map(_.get)

    val contextDF = sqlContext.createDataFrame(contextOnly)

    //placeholder for writing parquet files to HDFS

    verbDF.write.mode("append").parquet("/incoming/pocData/verb/verb.parquet")

    conversationDF.write.mode("append").parquet("/incoming/pocData/conv/conv.parquet")

    dialogDF.write.mode("append").parquet("/incoming/pocData/dial/dial.parquet")

    contextDF.write.mode("append").parquet("/incoming/pocData/cont/cont.parquet")

  }

}
