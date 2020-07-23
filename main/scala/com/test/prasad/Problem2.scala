package com.test.prasad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Problem2 extends App{



  val spark = SparkSession.builder().appName("App").config("spark.some.config.option", "360000").
    config("spark.master", "local[*]").getOrCreate()
  spark.conf.set("spark.sql.broadcastTimeout", 50000)
  spark.conf.set("spark.sql.session.timeZone", "UTC")


  // spark.sql("set spark.sql.caseSensitive=true")


  import spark.implicits._
  val excel_schema = StructType(Array(
    StructField("CROP_YEAR", StringType, nullable = true),
    StructField("AREA_HARVEST", StringType, nullable = true),
    StructField("YIELD", StringType, nullable = true),
    StructField("PRODUCTION", StringType, nullable = true),

    StructField("IMPORTS", StringType, nullable = true),
    StructField("EXPORTS", StringType, nullable = true),

    StructField("TOTAL_CONSUMPTION", StringType, nullable = true),
    StructField("UNACCOUNTED", StringType, nullable = true),
    StructField("ENDING_STOCKS", StringType, nullable = true)

  ))




  val Array_sheets=Array("Barley","Beef","Corn","Cotton","Pork","Poultry","Rice","Sorghum","Soybeans","Soybean meal","Soybean oil","Wheat");
  Array_sheets.foreach(elem=>{
    //for (elem <- Array_sheets) {
    System.out.println(elem)

    var df_excel = spark.sqlContext.read

      .format("com.crealytics.spark.excel")
      .option("sheetName", elem)
      .option("useHeader", "false")
      .option("treatEmptyValuesAsNulls", "false")
      .schema(excel_schema)
      .load("D:\\InternationalBaseline2019-Final.xlsx")
      .withColumn("id", monotonically_increasing_id()).cache()


    //  .filter($"CROP_YEAR".isNotNull.and(trim($"CROP_YEAR").startsWith("20")))

    val USDA_DATA_ONLY = df_excel.filter($"CROP_YEAR".contains(lit("USDA")))
      .withColumn("Dummy", lit(1)).cache();

    val USA_START = USDA_DATA_ONLY
      .filter($"CROP_YEAR".startsWith("USA"))
      .select($"id".as("USA_START_ID"), $"Dummy")


    USA_START.createOrReplaceTempView("USA_START");
    val USA_END = USDA_DATA_ONLY
      .join(USA_START, "Dummy")
      .where($"id".>($"USA_START_ID")).orderBy($"id".asc).limit(1)

      .select($"id".as("USA_END_ID"))
    USA_END.createOrReplaceTempView("USA_END");
    df_excel.createOrReplaceTempView("main_table");


    val USA = spark.sqlContext.sql(
      " select trim(CROP_YEAR)  as CROP_YEAR" +
        ", trim(AREA_HARVEST) as AREA_HARVEST " +
        "from main_table M  " +
        " left outer join  USA_START  S on 1=1 " +
        " left outer join  USA_END  E on 1=1 " +
        "where M.id>S.USA_START_ID and  M.id < E.USA_END_ID and CROP_YEAR IS NOT NULL AND trim(CROP_YEAR) like '20%'"

    ).withColumn("CROP_YEAR",when(not($"CROP_YEAR".like("%/%")),
      concat_ws("/",$"CROP_YEAR",
        substring($"CROP_YEAR",3,4).cast(IntegerType).+(1)
      )
    ).otherwise(trim($"CROP_YEAR"))
    )

    USA.createOrReplaceTempView("USA_PART")


    val WORLD_START = USDA_DATA_ONLY
      .filter($"CROP_YEAR".startsWith("WORLD"))
      .select($"id".as("WORLD_START_ID"), $"Dummy")
    WORLD_START.createOrReplaceTempView("WORLD")


    val WORLD = spark.sqlContext.sql(
      " select trim(CROP_YEAR) as CROP_YEAR, trim(AREA_HARVEST) as AREA_HARVEST " +
        "from main_table M  " +
        " left outer join  WORLD  W on 1=1 " +

        "where M.id>W.WORLD_START_ID AND CROP_YEAR IS NOT NULL AND trim(CROP_YEAR) like '20%'"

    ).withColumn("CROP_YEAR",when(not($"CROP_YEAR".like("%/%")),
      concat_ws("/",$"CROP_YEAR",
        substring($"CROP_YEAR",3,4).cast(IntegerType).+(1)
      )
    ).otherwise(trim($"CROP_YEAR"))
    )

    WORLD.createOrReplaceTempView("WORLD_PART")




    val COMBINED = spark.sqlContext.sql("Select WORLD_PART.CROP_YEAR as  CROP_YEAR," +
      "WORLD_PART.AREA_HARVEST as world_harvest," +
      "(cast(USA_PART.AREA_HARVEST as float)/cast (WORLD_PART.AREA_HARVEST as float))*100  as usa_contribution" +

      " from " +
      "  WORLD_PART LEFT OUTER JOIN USA_PART ON    WORLD_PART.CROP_YEAR=USA_PART.CROP_YEAR  " ).cache()
    COMBINED.createOrReplaceTempView("COMBINED_"+elem.replaceAll(" ","_").toUpperCase)

    // USA.show(1000, false);

    // WORLD.show(1000, false)
    // COMBINED.show(1000, false)

    spark.catalog.dropTempView("USA_END")
    spark.catalog.dropTempView("USA_START")
    spark.catalog.dropTempView("WORLD")
    spark.catalog.dropTempView("USA_PART")
    spark.catalog.dropTempView("WORLD_PART")

  })

  //Joining all so far

  val final_df = spark.sqlContext.sql("Select COMBINED_BARLEY.CROP_YEAR as  Year," +
    "COMBINED_BARLEY.world_harvest as world_barley_harvest," +
    "COMBINED_BARLEY.usa_contribution  as `usa_barley_contribution%`," +

    //Beef
    "COMBINED_BEEF.world_harvest as world_beef_harvest," +
    "COMBINED_BEEF.usa_contribution  as `usa_beef_contribution%`," +

    //corn
    "COMBINED_CORN.world_harvest as world_corn_harvest," +
    "COMBINED_CORN.usa_contribution  as `usa_corn_contribution%`," +

    //cotton
    "COMBINED_COTTON.world_harvest as world_cotton_harvest," +
    "COMBINED_COTTON.usa_contribution  as `usa_cotton_contribution%`," +

    //PORK
    "COMBINED_PORK.world_harvest as world_pork_harvest," +
    "COMBINED_PORK.usa_contribution  as `usa_pork_contribution%`," +
    //POULTRY
    "COMBINED_POULTRY.world_harvest as world_poultry_harvest," +
    "COMBINED_POULTRY.usa_contribution  as `usa_poultry_contribution%`," +

    //RICE
    "COMBINED_RICE.world_harvest as world_rice_harvest," +
    "COMBINED_RICE.usa_contribution  as `usa_rice_contribution%`," +


    //SORGHUM
    "COMBINED_SORGHUM.world_harvest as world_sorghum_harvest," +
    "COMBINED_SORGHUM.usa_contribution  as `usa_sorghum_contribution%`," +

    //SOYBEANS
    "COMBINED_SOYBEANS.world_harvest as world_soybeans_harvest," +
    "COMBINED_SOYBEANS.usa_contribution  as `usa_soybeans_contribution%`," +



    //SOYBEAN_MEAL
    "COMBINED_SOYBEAN_MEAL.world_harvest as world_soybean_meal_harvest," +
    "COMBINED_SOYBEAN_MEAL.usa_contribution  as `usa_soybean_meal_contribution%`," +
    //SOYBEAN_OIL
    "COMBINED_SOYBEAN_OIL.world_harvest as world_soybean_oil_harvest," +
    "COMBINED_SOYBEAN_OIL.usa_contribution  as `usa_soybean_oil_contribution%`," +
    //WHEAT
    "COMBINED_WHEAT.world_harvest as world_wheat_harvest," +
    "COMBINED_WHEAT.usa_contribution  as `usa_wheat_contribution%`" +

    " from " +
    "  COMBINED_BARLEY LEFT OUTER JOIN COMBINED_BEEF ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_BEEF.CROP_YEAR  " +

    "  LEFT OUTER JOIN COMBINED_CORN ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_CORN.CROP_YEAR  " +
    "  LEFT OUTER JOIN COMBINED_COTTON ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_COTTON.CROP_YEAR  " +
    " LEFT OUTER JOIN COMBINED_PORK ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_PORK.CROP_YEAR  " +
    " LEFT OUTER JOIN COMBINED_RICE ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_RICE.CROP_YEAR  " +
    "   LEFT OUTER JOIN COMBINED_POULTRY ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_POULTRY.CROP_YEAR  " +
    " LEFT OUTER JOIN COMBINED_SOYBEANS ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_SOYBEANS.CROP_YEAR  " +
    "  LEFT OUTER JOIN COMBINED_SORGHUM ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_SORGHUM.CROP_YEAR  " +
    "  LEFT OUTER JOIN COMBINED_SOYBEAN_MEAL ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_SOYBEAN_MEAL.CROP_YEAR  " +
    " LEFT OUTER JOIN COMBINED_SOYBEAN_OIL ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_SOYBEAN_OIL.CROP_YEAR  " +
    "   LEFT OUTER JOIN COMBINED_WHEAT ON    COMBINED_BARLEY.CROP_YEAR=COMBINED_WHEAT.CROP_YEAR  " +
    " order by 1" +
    "")



  //final_df.show(1000, false)
  final_df.write.option("delimiter","|")
    .option("header", "true").mode(SaveMode.Overwrite)
    .   format("csv").save("D:\\FINAL_REPORT.txt")







}
