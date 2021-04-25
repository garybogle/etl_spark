package org.garybogle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, concat_ws, desc}

object SayariAssignment {
  val sdnHeader = Seq("ent_num",
                      "SDN_Name",
                      "SDN_Type",
                      "Program",
                      "Title",
                      "Call_Sign",
                      "Vess_type",
                      "Tonnage",
                      "GRT",
                      "Vess_flag",
                      "Vess_owner",
                      "Remarks")

  val addHeader = Seq("Ent_num",
                      "Add_num",
                      "Address",
                      "City/State/Province/PostalCode",
                      "Country",
                      "Add_remarks")

  val altHeader = Seq("ent_num",
                      "alt_num",
                      "alt_type",
                      "alt_name",
                      "alt_remarks")

  // The US SDN csv files do not have a header row, however, the UK ConList file does

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Sayari Assignment")
      .getOrCreate()

    // The csv files should be in the same directory that
    // spark-submit is run from. The files should be listed
    // on the command line at the end of the spark-submit call
    val sdnFile = args(0)
    val addressFile = args(1)
    val altNamesFile = args(2)
    val conListFile = args(3)

    // Read in the main SDN file
    val sdnDfWithoutHeaders = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load(sdnFile)

    // Add headers to the main SDN file
    val sdnDf = sdnDfWithoutHeaders.toDF(sdnHeader:_*)

    // Read in the SND Address file
    val addDfWithoutHeaders = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load(addressFile)

    // Add headers to the SDN Address file
    val addDf = addDfWithoutHeaders.toDF(addHeader:_*)

    // Read in the SDN Alternate names file
    val altDfWithoutHeaders = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load(altNamesFile)

    // Add headers to the SDN Alternate names file
    val altDf = altDfWithoutHeaders.toDF(altHeader:_*)

    // Read in the UK conlist file
    val conListDf = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(conListFile)

    // Select the columns from the ConList that will be in the joint schema
    // Create a single Name and Address column. Create a single column
    // with city state and postal code as it is in the SDN file
    val conListJoinedNoOtherNames = conListDf.select(concat_ws(" ",
                                                   col("Name 6"),
                                                   col("Name 1"),
                                                   col("Name 2"),
                                                   col("Name 3"),
                                                   col("Name 4"),
                                                   col("Name 5")).as("Name"),
                                         concat_ws(" ",
                                                   col("Address 1"),
                                                   col("Address 2"),
                                                   col("Address 3"),
                                                   col("Address 4"),
                                                   col("Address 5")).as("Address"),
                                         concat_ws(" ",
                                                   col("Address 6"),
                                                   col("Post/Zip Code")).as("City/State/Province/PostalCode"),
                                         col("Country"),
                                         col("Group Type").as("EntityType"),
                                         col("Title"),
                                         col("Alias Type"),
                                         col("Name 5").as("OtherNames"),
                                         col("Regime").as("Program"),
                                         col("DOB"),
                                         concat_ws(" ", col("Town of Birth"), col("Country of Birth")).as("Birthplace"),
                                         col("Nationality"),
                                         col("Passport Details").as("PassportDetails"))

    // Other Names column to match a column in SDN files
    val conListJoinedNoSource = conListJoinedNoOtherNames.withColumn("OtherNames", lit(null))

    // Join the separate SDN files into one dataframe
    val sdn_joint = sdnDf.join(addDf, sdnDf("ent_num") === addDf("Ent_num"), "inner")
      .join(altDf, sdnDf("ent_num") === altDf("ent_num"), "inner")

    // Select the columns from the SDN combined dataframe for the Joint schema
    val sdnJoinedShort = sdn_joint.select(col("SDN_Name").as("Name"),
                                     col("Address"),
                                     col("City/State/Province/PostalCode"),
                                     col("Country"),
                                     col("SDN_Type").as("EntityType"),
                                     col("Title"),
                                     col("alt_type").as("AliasType"),
                                     col("alt_name").as("OtherNames"),
                                     col("Program"))

    // Create matching columns in the SDN dataframe for the Joint Schema
    // Mark these rows with the Source "SDN"
    val sdnJoined = sdnJoinedShort.withColumn("DOB", lit(null))
      .withColumn("Birthplace", lit(null))
      .withColumn("Nationality", lit(null))
      .withColumn("PassportDetails", lit(null))
      .withColumn("Source", lit("SDN"))

    // Mark these rows with the Source "ConList"
    val conListJoined = conListJoinedNoSource.withColumn("Source", lit("ConList"))

    // Join the two Sources into a single Dataframe
    val merged = sdnJoined.union(conListJoined).distinct()

    // Coalesce into a single partition and write the file out
    // as a single, merged parquet file
    merged.coalesce(1).write.mode("overwrite").parquet("assignment_output.parquet")

    spark.stop() 
  }
}
