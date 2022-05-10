package ru.scala78.spark.udf_research

import NativeUDF._

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.{
  SparkSessionExtensions,
  SparkSessionExtensionsProvider
}

class SessionExtensionsWithoutLoader extends SparkSessionExtensionsProvider {
  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectFunction(
      (
        new FunctionIdentifier("hash_xx3_nudf"),
        new ExpressionInfo(classOf[Hash].getName, "hash_xx3_nudf"),
        (children: Seq[Expression]) => Hash(children.head)
      )
    )
    v1.injectFunction(
      (
        new FunctionIdentifier("addition_nudf"),
        new ExpressionInfo(classOf[Addition].getName, "addition_nudf"),
        (children: Seq[Expression]) => Addition(children.head, children.last)
      )
    )
  }
}
