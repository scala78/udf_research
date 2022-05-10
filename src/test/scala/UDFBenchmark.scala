package org.apache.spark.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.scala78.spark.udf_research.SessionExtensionsWithoutLoader
import ru.scala78.spark.udf_research.UDF._

object UDFBenchmark extends BenchmarkBase {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .config(
      "spark.sql.extensions",
      classOf[SessionExtensionsWithoutLoader].getName
    )
    .getOrCreate()
  val startRange: Long = 100000L
  val endRange: Long = 500000L
  val data: DataFrame = spark.range(startRange, endRange).toDF("c").cache()

  data.createOrReplaceTempView("vw_test")
  spark.sql("select * from vw_test").collect()

  additionUDFReg(spark)
  hashXx3UDFReg(spark)
  val iterations: Int = 10

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit =
    benchmarkCompute(iterations)

  private def benchmarkCompute(iterations: Int): Unit = {

    val benchmark =
      new Benchmark("Comparison of the use of UDF and NativeUDF", iterations)
    benchmark.addCase("using UDF", iterations) { _ =>
      spark
        .sql("select c, hash_xx3_udf(addition_udf(c,10L)) from vw_test")
        .collect()
      ()
    }
    benchmark.addCase("using NativeUDF", iterations) { _ =>
      spark
        .sql("select c, hash_xx3_nudf(addition_nudf(c,10L)) from vw_test")
        .collect()
      ()
    }
    benchmark.run()
  }
}
