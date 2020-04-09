package com.tdp.genesis.spark

import org.apache.avro.Schema
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, first, hour, lit, sum, to_utc_timestamp, unix_timestamp, count, when}
import org.apache.spark.sql.types.{LongType, TimestampType}
import com.databricks.spark.avro._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.Logger
import org.apache.log4j.Level


object DataConsumption {

  private val dateFmt = "yyyy/MM/dd hh:mm:ss a"

  // TIME FUNCTION
  def today(): String = {
    val date = new Date
    val sdf = new SimpleDateFormat(dateFmt)
    sdf.format(date)
  }

  def main(args: Array[String]): Unit = {

    println(
      s"""
         |
         | ====================================================================
         | =  Starting   SPARK                                                =
         | =  Proceso:         DataConsumption                                =
         | =  Log Level:       OFF                                            =
         | =  Fecha de Inicio: ${today()}                         =
         | ====================================================================
         |
         |""".stripMargin)


    // parameters
    val pathFile    = args(0)
    val outPath     = args(1)
    val schemaFile  = args(2)
    val appName     = args(3)
    val appMode     = args(4)
    val splitter    = args(5)
    val serviceCode = args(6)
    val errorPath   = args(7)
    val successfulPath = args(8)
    val partitions  = args(9)
    //val compression = args(10)

    Logger.getLogger("org").setLevel(Level.OFF)    //Show Error
    //Logger.getLogger("akka").setLevel(Level.OFF)        //Show Info

    val spark = SparkSession.builder()
      .master(appMode)
      .appName(appName)
      .getOrCreate()

    val sc = spark.sparkContext
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    spark.conf.set("spark.sql.shuffle.partitions", partitions)        // number of partitions
    //spark.conf.set("spark.sql.avro.compression.codec", compression)   // snappy , uncompressed

    // Schema Avro
    val avscFile = sc.wholeTextFiles(schemaFile)
    val schemaAvro = new Schema.Parser().parse(avscFile.take(1)(0)._2)
    val parameters = Map("avroSchema" -> schemaAvro.toString())


    // Read CRDs
    sc.wholeTextFiles(pathFile).collect.foreach {
      hdfspartition =>
        try {
          val filenamesArray = hdfspartition._2.split("\\r?\\n")   // create Array  cdr.DAT
          val filenamesString = filenamesArray.mkString(",")      // string from an array


          val cdr = sc.textFile(filenamesString)
            .map(line => line.split(splitter, -1))
            .map(x => Row.fromSeq(x))


          val df = spark.createDataFrame(cdr, CdrSchema.dfSchema)
          df.createOrReplaceTempView("dfTable")
          // df.show(100,false)

          val df2 = df.select(
            concat(col("NumOrigen"), lit("-"),
              col("CodigoTipoProductoTasador"),
              lit("-"),
              col("FechaInicio"), lit("-"),
              hour(to_utc_timestamp(unix_timestamp(concat(col("FechaInicio"),lit(" "), hour(col("HoraInicio")), lit(":00:00")), "yyyyMMdd HH:mm:ss").cast(TimestampType), "Z")),
              lit(":00:00"),lit("-"),
              lit(hdfspartition._1.takeRight(13))).alias("id"),
            lit("Datos").alias("name"),
            lit("Datos").alias("description"),
            col("NumOrigen").alias("phone_number"),
            lit("data_consumption").alias("event_type"),
            col("CodigoTipoProductoTasador").alias(("quota_id")),
            lit(true).alias("amount_tax_included"),
            lit("hour").alias("aggregation_type"),
            col("FechaInicio").alias("FechaInicio"),
            hour(col("HoraInicio")).alias("HoraInicio"),
            to_utc_timestamp(unix_timestamp(concat(col("FechaInicio"), lit(" "), hour(col("HoraInicio")), lit(":00:00")), "yyyyMMdd HH:mm:ss").cast(TimestampType), "Z").alias("UTC"),
            col("CantidadBytesEnviados"),
            col("CantidadBytesRecibidos"),
            col("ValorReal"),
            col("CodigoTipoServicioTrafico")
          ).filter(col("CodigoTipoProductoTasador").isin(List("0", "1", "2", "3", "4", "5"): _*) && col("CodigoTipoServicioTrafico") === serviceCode)


          val df3 = df2
            .na.replace("CantidadBytesEnviados", Map("" -> "0"))
            .na.replace("CantidadBytesRecibidos", Map("" -> "0"))


          // EJECUTAMOS LAS AGREGACIONES
          val df4 = df3.select("phone_number", "quota_id", "FechaInicio", "HoraInicio", "CantidadBytesEnviados", "CantidadBytesRecibidos", "ValorReal", "id", "name", "description", "event_type", "amount_tax_included", "aggregation_type", "UTC")
            .groupBy ("phone_number", "quota_id", "FechaInicio", "HoraInicio")
            .agg(
              sum(col("CantidadBytesEnviados").cast(LongType) + col("CantidadBytesRecibidos").cast(LongType)).alias("bytes"),
              first("UTC").alias("creation_date"),
              sum(col("ValorReal").cast(LongType)).alias("amount_value"),
              first("id").alias("id"),
              first("name").alias("name"),
              first("description").alias("description"),
              first("event_type").alias("event_type"),
              first("amount_tax_included").alias("amount_tax_included"),
              first("aggregation_type").alias("aggregation_type")
            ).select("id", "name", "description", "phone_number", "event_type", "creation_date", "bytes", "quota_id", "amount_value", "amount_tax_included", "aggregation_type")


          val df5 = df4.withColumn("quota_id",
            when(col("quota_id") === "0" , "Bonos")
              .when(col("quota_id") === "1" , "Supercargas")
              .when(col("quota_id") === "2" , "Paquetes")
              .when(col("quota_id") === "3" , "Cargo Fijo")
              .when(col("quota_id") === "4" , "Limite de Consumo")
              .when(col("quota_id") === "5" , "Bolsa")
          )

          df5.show(5,false)

          // SERIALIZAMOS LA DATA Y GUARDAMOS .AVRO
          df5.write.options(parameters)
            .mode(SaveMode.Append)
            .avro(outPath)

          val source = new Path(hdfspartition._1)
          val target = new Path(successfulPath)
          fs.rename(source, target)                                   //cambio de ruta HDFS
          if (fs.exists(source)) fs.delete(source,false)     //Remove file


          println(
            s"""
               |
               |
               |----------------------------------------------------------------------------
               || Path Origen: $source
               || Path OK:     $target
               |----------------------------------------------------------------------------
               | --->  END, Status: OK
               |  ==========================================================================
               |""".stripMargin)

        }

        catch {
          case e: Exception => println(
            s"""
               |--->  ERROR catch
               |$e
               |
               |""".stripMargin);
            val source = new Path(hdfspartition._1)
            val target = new Path(errorPath)

            fs.rename(source, target)                                 //cambio de ruta HDFS
            if (fs.exists(source)) fs.delete(source,false)  //Remove file

            println(
              s"""
                 |
                 |------------------------------------------------------------------------
                 || Path Origen: $source
                 || Path Error:  $target
                 |------------------------------------------------------------------------
                 |
                 |  --->  END, Status: ERROR
                 |===================================================================================
                 |""".stripMargin)

        }

        finally {
          // Something else
        }
    }

    println(
      s"""
         |
         | ================================================================
         | =  --->  Stopping   SPARK                                      =
         | =  Process:  DataConsumption                                   =
         | =  End Date: ${today()}                            =
         | ================================================================
         |
         |
         |""".stripMargin)

  }
}
