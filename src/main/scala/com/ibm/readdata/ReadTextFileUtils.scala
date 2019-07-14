package com.ibm.readdata


import java.io.File
import java.util.{Calendar, UUID}

import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object ReadTextFileUtils {

  val LOG = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    LOG.setLevel(Level.ERROR)
    val sparkSession = SparkSession.builder().appName("ReadFile").master("local[*]")
      .config("spark.sql.warehouse.dir", "C:\\Users\\GirishBamane\\Desktop\\Study\\warehouse")
      .getOrCreate()
    val fileName: String = "C:\\Users\\GirishBamane\\Desktop\\Study\\Input\\inputjson.json"

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val myRDD = sparkContext.textFile(fileName)

    def md(Name: String): Row = {
      val name = Name.toLowerCase()
      Row("2", "A", name)
    }

    val data = myRDD.map(md)

    val schema = StructType(List(
      StructField("roll", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("payload", StringType, nullable = true)
    ))

    val df = sqlContext.createDataFrame(data, schema)
    df.write.mode("append").parquet("")


  }

}