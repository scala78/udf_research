package ru.scala78.spark.udf_research

import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.catalyst.expressions.{
  BinaryExpression,
  ExpectsInputTypes,
  Expression,
  NullIntolerant,
  UnaryExpression
}
import org.apache.spark.sql.types.{DataType, LongType}

/** Factory for [[ru.scala78.spark.udf_research.NativeUDF]] instances. */
object NativeUDF {
  case class Hash(input: Expression)
      extends UnaryExpression
      with ExpectsInputTypes
      with NullIntolerant {
    override def inputTypes: Seq[LongType] = Seq(LongType)

    override def dataType: DataType = LongType

    override def child: Expression = input

    override def prettyName: String = "hash_xx3_nudf"

    override protected def doGenCode(
        ctx: CodegenContext,
        ev: ExprCode
    ): ExprCode = {
      defineCodeGen(
        ctx,
        ev,
        c => s"net.openhft.hashing.LongHashFunction.xx3().hashLong($c)"
      )
    }

    override protected def nullSafeEval(str: Any): Any = {
      net.openhft.hashing.LongHashFunction
        .xx3()
        .hashLong(str.asInstanceOf[Long]);
    }

    override protected def withNewChildInternal(
        newChild: Expression
    ): Expression = copy(input = newChild)
  }

  case class Addition(left: Expression, right: Expression)
      extends BinaryExpression
      with ExpectsInputTypes
      with NullIntolerant {

    override def inputTypes: Seq[LongType] = Seq(LongType, LongType)

    override def dataType: DataType = LongType

    override def prettyName: String = "addition_nudf"

    override protected def doGenCode(
        ctx: CodegenContext,
        ev: ExprCode
    ): ExprCode =
      defineCodeGen(ctx, ev, (l, r) => s"$l + $r")

    override protected def nullSafeEval(left: Any, right: Any): Any = {
      left.asInstanceOf[Long] + right.asInstanceOf[Long]
    }

    override protected def withNewChildrenInternal(
        newLeft: Expression,
        newRight: Expression
    ): Expression =
      copy(left = newLeft, right = newRight)
  }

}
