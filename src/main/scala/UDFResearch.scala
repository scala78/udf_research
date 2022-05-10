package ru.scala78.spark.udf_research

import UDF._

import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Factory for [[ru.scala78.spark.udf_research.UDFResearch]] instances. */
object UDFResearch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config(
        "spark.sql.extensions",
        classOf[SessionExtensionsWithoutLoader].getName
      )
      .getOrCreate()
    org.apache.spark.SparkEnv.get.conf.set("spark.sql.codegen.comments", "true")

    additionUDFReg(spark)
    hashXx3UDFReg(spark)
    val startRange: Long = 100000L
    val endRange: Long = 500000L
    val data: DataFrame = spark.range(startRange, endRange).toDF("c").cache()

    data.createOrReplaceTempView("vw_test")
    spark.sql("select * from vw_test").collect()

    // Example using UDF
    val dfUDF =
      spark.sql("select c, hash_xx3_udf(addition_udf(c,10L)) from vw_test")
    dfUDF.explain(extended = true)
    dfUDF.debugCodegen()

    // Example using Native UDF
    val dfNativeUDF =
      spark.sql("select c, hash_xx3_nudf(addition_nudf(c,10L)) from vw_test")
    dfNativeUDF.explain(extended = true)
    dfNativeUDF.debugCodegen()

  }

}
