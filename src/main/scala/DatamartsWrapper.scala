/**
  * Created by a.velychko on 1/11/19.
  */

package com.anchorfree.datamart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object DatamartsWrapper {

  val logger = Logger.getLogger(getClass.getName)

  val SQL_FILE_NAME = "sql_file_name"
  val TEST = "test"
  val START_DATE = "start_date"
  val END_DATE = "end_date"

  var test = false

  object Spark {
    var appName = "Airflow Datamarts Wrapper"

    lazy val sc = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    lazy val conf = new Configuration()
    lazy val fileSystem = FileSystem.get(conf)
  }

  def replaceSchemaWithTest(mainStatement: String): String = {
    val testReplacements = Map(
      "ods." -> "ods_test.",
      "dwh." -> "dwh_test.",
      "presentation." -> "presentation_test.",                                          
      "datamart." -> "datamart_test.")
    testReplacements.foldLeft(mainStatement)((a, b) => a.replaceAllLiterally(b._1, b._2))
  }

  def runQuery(query: String, startDate: String = null, endDate: String = null, sqlContext: SparkSession) = {

    var queryFinal = if (test) replaceSchemaWithTest(query) else query

    val (startDateFinal, endDateFinal) = {
      import java.time.{ZonedDateTime, ZoneId}
      import java.time.format.DateTimeFormatter

      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)
      (Option(startDate).getOrElse(formatter.format(yesterday)), Option(endDate).getOrElse(formatter.format(yesterday)))
    }

    queryFinal = queryFinal.replaceAll("\\{START_DATE\\}", startDateFinal)
        .replaceAll("\\{END_DATE\\}", endDateFinal)

    logger.log(Level.INFO, "RUNNING: " + queryFinal)
    println(queryFinal)

    Spark.sc.sql(queryFinal)
  }

  def excludeComments(text: List[String]): List[String] = {
    text.filter(!_.equals("--")).flatMap(x =>
      if (x.contains("--")) {
        List(x.split("--").head).map(_.trim).filter(x => x.length > 0)
      } else {
        List(x).map(_.trim)
      }
    )
  }

  def getQueriesFromFile(filePath: String): Array[String] = {
    logger.log(Level.INFO, "Reading file " + filePath)
    val fileContent = Spark.sc.read.textFile(filePath).collect().toList
    val fileContentWoComments = excludeComments(fileContent).map(_.toString).mkString(" ")

    val queriesList1 = fileContentWoComments.split("(?<!\\\\|');")
    for( x <- queriesList1) {
      println(x)
    }

    return queriesList1
  }


  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption(SQL_FILE_NAME, true, "")
    options.addOption(TEST, true, "")
    options.addOption(START_DATE, true, "")
    options.addOption(END_DATE, true, "")

    val parser = new BasicParser
    val cmd = parser.parse(options, args)

    val sqlFileName = cmd.getOptionValue(SQL_FILE_NAME)
    val startDate: String = cmd.getOptionValue(START_DATE)
    val endDate: String = cmd.getOptionValue(END_DATE)
    test = cmd.getOptionValue(TEST, "false").toBoolean

    if (test) {
      logger.log(Level.INFO, " ---------------------- TEST RUN ------------------")
    }

    val appName = s"Airflow Datamarts Wrapper: file=${sqlFileName}, start_date=${startDate}"
    Spark.appName = if (test) "Test: " + appName else appName

    val sparkContext = Spark.sc.sparkContext
    var stagingDir = System.getenv("SPARK_YARN_STAGING_DIR")
    if (stagingDir == null) {
      stagingDir = ".sparkStaging/" + sparkContext.applicationId
    }
    val sqlPathOnHdfs = stagingDir + "/" + sqlFileName

    val queries = getQueriesFromFile(sqlPathOnHdfs)
    for (query <- queries) {
      runQuery(query, startDate, endDate, Spark.sc)
    }
  }
}
