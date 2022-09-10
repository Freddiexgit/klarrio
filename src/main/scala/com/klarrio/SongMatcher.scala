package com.klarrio

import com.github.vickumar1981.stringdistance.StringDistance._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.nio.file.Path
import scala.reflect.io.Directory
import scala.util.matching.Regex

object SongMatcher {
  val inputFile = "files/input/input_file.csv"
  val songCode = "files/input/SongCodeDB2.csv"
  val lookupKey = "files/input/lookupKeyDB1.csv"
  val matchedExample = "files/input/Matched400.csv"

  def main(args: Array[String]) {
    clearResultFromPrevoiseRun()
    val spark = SparkSession.builder()
      .appName("SongMatcher")
      .master("local[*]")
      .getOrCreate()

    // load input file from  "files" folder, output matched file to "files/output"
    matchSongs(spark)

  }

  // match the songs by lookup key, pickup the exact matches output into a file
  // from the duplicated matched use writer name to do a  string fuzzy match then output to another file
  def matchSongs(spark: SparkSession): Unit = {
    val inputDF = loadFile(spark, inputFile).drop("input_performers")
    val lookupKeyDF = loadFile(spark, lookupKey).drop("usage_count")
    val songCodeDF = loadFile(spark, songCode)
    val matchedExampleDF = loadFile(spark, matchedExample).select("unique_input_record_identifier","lookup_key")
    inputDF.createOrReplaceTempView("input")
    val addRecordNumberSql =
      """
        | select  row_number() OVER (  PARTITION BY ''   ORDER BY ''  ) as unique_input_record_ , * from input
        |""".stripMargin
    val inputFileDF = spark.sql(addRecordNumberSql)
    inputFileDF.createOrReplaceTempView("input_file")
    lookupKeyDF.createOrReplaceTempView("lookup_key")
    songCodeDF.createOrReplaceTempView("song_code")
    matchedExampleDF.createOrReplaceTempView("matched_example")
    val titleToLookupKeyUdf = udf(titleToLookupKey)
    spark.udf.register("titleToLookupKeyUdf", titleToLookupKeyUdf)

    val matchedWithDuplicates ="""select
                                 |unique_input_record_identifier,
                                 |count(*)  as count
                                 |from
                                 |input_file left join lookup_key
                                 |on  titleToLookupKeyUdf(input_file.input_title) = lookup_key.lookup_key
                                 |where database_song_code is not null
                                 |group by unique_input_record_identifier """.stripMargin


    val matchedWithDuplicatesDF = spark.sql(matchedWithDuplicates)
    matchedWithDuplicatesDF.createOrReplaceTempView("matchFoundWithDuplicates")
    titleAndLookupKeyExactMatch(spark)
    titleAndLookupKeyWithDuplicates(spark)
  }

  def clearResultFromPrevoiseRun(): Unit = {
    val p = Path.of("files/output")
    if (p != null) {
      val dir = new Directory(p.toFile)
      dir.deleteRecursively()
    }
  }

  def titleAndLookupKeyWithDuplicates(spark: SparkSession) = {
    // to use song writer fuzzy match when duplicated matches found
    val duplicatedMatch = "select unique_input_record_identifier from matchFoundWithDuplicates  where count > 1"
    val duplicatedMatchDF = spark.sql(duplicatedMatch)
    duplicatedMatchDF.createOrReplaceTempView("duplicatedMatch")

    val stringFuzzyMatchUdf = udf(stringFuzzyMatch)
    spark.udf.register("stringFuzzyMatchUdf", stringFuzzyMatchUdf)
    val mapping = """select
                    |unique_input_record_,
                    |input_title ,
                    |titleToLookupKeyUdf(input_title) as lookup_key ,
                    |matched_example.lookup_key as lookup_key_given ,
                    |database_song_code,
                    |database_song_title ,
                    |database_song_writers
                    |from
                    |(
                    |	select
                    |	input_file.unique_input_record_,
                    | input_file.unique_input_record_identifier,
                    |	input_title ,
                    |	code.database_song_code,
                    |	code.database_song_title ,
                    |	code.database_song_writers   ,
                    |	stringFuzzyMatchUdf(input_writers, database_abb05v_writers) matchScore
                    |	from
                    |	input_file ,
                    |	duplicatedMatch,
                    |	song_code code,
                    |	lookup_key  key
                    |	where
                    |	titleToLookupKeyUdf(input_file.input_title) = key.lookup_key
                    |	and key.database_song_code = code.database_song_code
                    |	and input_file.unique_input_record_identifier = duplicatedMatch.unique_input_record_identifier ) a
                    |	left join matched_example on a.unique_input_record_identifier = matched_example.unique_input_record_identifier
                    |	where
                    |		(a.unique_input_record_identifier , a.matchScore) in
                    |			(select input_file.unique_input_record_identifier ,	max(stringFuzzyMatchUdf(input_writers, database_abb05v_writers)) matchScore
                    |				from input_file ,
                    |				duplicatedMatch,
                    |				song_code code,
                    |				lookup_key  key
                    |				where titleToLookupKeyUdf(input_file.input_title) = key.lookup_key
                    |				and key.database_song_code = code.database_song_code
                    |				and input_file.unique_input_record_identifier = duplicatedMatch.unique_input_record_identifier
                    |				group by input_file.unique_input_record_identifier
                    |			)
                    |  order by a.unique_input_record_ """.stripMargin

    writeOutData(spark.sql(mapping), true, "titleAndLookupKeyAndWriterFuzzyMatch")

  }

  def titleAndLookupKeyExactMatch(spark: SparkSession) = {

    val singleMatch = "select unique_input_record_identifier from matchFoundWithDuplicates  where count = 1"
    val singleMatchDF = spark.sql(singleMatch)
    singleMatchDF.createOrReplaceTempView("singleMatch")
    val mapping = """select
                    |input_file.unique_input_record_ ,
                    |input_title ,
                    |titleToLookupKeyUdf(input_file.input_title) as lookup_key ,
                    |matched_example.lookup_key as lookup_key_given ,
                    |code.database_song_code,
                    |code.database_song_title ,
                    |code.database_song_writers
                    |from input_file  left join matched_example on input_file.unique_input_record_identifier=matched_example.unique_input_record_identifier,
                    |singleMatch,
                    |song_code code,
                    |lookup_key  key
                    |where
                    |titleToLookupKeyUdf(input_file.input_title) = key.lookup_key
                    |and key.database_song_code = code.database_song_code
                    |and input_file.unique_input_record_identifier = singleMatch.unique_input_record_identifier
                    |order by input_file.unique_input_record_""".stripMargin
    writeOutData(spark.sql(mapping), true, "titleAndLookupKeyExactMatch")

  }


  val  stringFuzzyMatch= (str1: String, str2: String) => {

    //      Overlap.score(str1, str2, 1)
    //      NGram.score(str1, str2, 4)
    Cosine.score(str1, str2)
  }

  def writeOutData(df: DataFrame, shouldWriteHeader: Boolean = false, fileName: String) = {
    df.coalesce(1)
      .write.mode(SaveMode.Append)
      .option("header", shouldWriteHeader)
      .option("delimiter", "\t")
      .csv(s"files/output/$fileName")


  }

  // matching code is fixed length 25, all uppercase
  val  titleToLookupKey = (inputTile: String) => {
    val reg = new Regex("[0-9a-zA-Z&]")
    var str = inputTile.map(x => {
      reg.findFirstMatchIn(x.toString) match {
        case Some(_) => x
        case None => ""
      }
    })
      .map(x => x.toString.toUpperCase())
      .mkString
    if (str.length > 25)
      str = str.substring(0, 25)

    str.padTo(25, " ").mkString
  }

  def loadFile(spark: SparkSession, fileName: String): DataFrame = {
    spark.read.option("delimiter", "\t")
      .option("header", "true")
      .option("encoding", "UTF8")
      .csv(fileName)
  }

}

