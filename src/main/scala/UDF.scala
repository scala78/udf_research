package ru.scala78.spark.udf_research

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** Factory for [[ru.scala78.spark.udf_research.UDF]] instances. */
object UDF {
  val hashXx3UDF: UserDefinedFunction =
    udf((in: Long) => net.openhft.hashing.LongHashFunction.xx3().hashLong(in))
  val additionUDF: UserDefinedFunction =
    udf((left: Long, right: Long) => left + right)

  def additionUDFReg(spark: SparkSession): Unit =
    spark.udf.register("addition_udf", additionUDF)

  def hashXx3UDFReg(spark: SparkSession): Unit =
    spark.udf.register("hash_xx3_udf", hashXx3UDF)
}
