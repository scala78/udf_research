package ru.scala78.spark

import org.apache.spark.sql.Column
import ru.scala78.spark.udf_research.NativeUDF._

package object udf_research {

  def additionNUDF(left: Column, right: Column): Column = new Column(
    Addition(left.expr, right.expr)
  )

  def hashXx3NUDF(c: Column): Column = new Column(Hash(c.expr))
}
