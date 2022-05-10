package ru.scala78.spark.udf_research

import org.apache.spark.{LocalSparkContext, SparkFunSuite}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{Row, SparkSession}

class NativeUDFTest extends SparkFunSuite with LocalSparkContext {
  override val enableAutoThreadAudit = false

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .config(
      "spark.sql.extensions",
      classOf[SessionExtensionsWithoutLoader].getName
    )
    .getOrCreate()

  val data = Seq((1000L, 10000L), (1L, 1L), (1L, 4L))

  import spark.implicits._

  test("Extension add long literal to long literal") {
    data.toDF("c1", "c2").createOrReplaceTempView("test_data")
    checkAnswer(
      spark.sql("select c1, c2, addition_nudf(c1,c2) from test_data"),
      Row(1000L, 10000L, 11000L)
        :: Row(1L, 1L, 2L)
        :: Row(1L, 4L, 5L)
        :: Nil
    )
  }
  test("Extension compute hash xx3") {
    data.toDF("c1", "c2").createOrReplaceTempView("test_data")
    checkAnswer(
      spark.sql(
        "select c1, c2, hash_xx3_nudf(c1),hash_xx3_nudf(c2) from test_data"
      ),
      Row(1000L, 10000L, -8425515611983501939L, -7482769108513497636L)
        :: Row(1L, 1L, 3439722301264460078L, 3439722301264460078L)
        :: Row(1L, 4L, 3439722301264460078L, -3881494802266689160L)
        :: Nil
    )
  }
  test("Extension compute hash xx3 and adding long literals") {
    data.toDF("c1", "c2").createOrReplaceTempView("test_data")
    checkAnswer(
      spark.sql(
        "select c1, c2, hash_xx3_nudf(addition_nudf(c1,c2)) from test_data"
      ),
      Row(1000L, 10000L, -1054776427242670003L)
        :: Row(1L, 1L, 2343778756980564547L)
        :: Row(1L, 4L, -8213464378072455284L)
        :: Nil
    )
  }
}
