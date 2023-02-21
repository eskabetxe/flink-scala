package pro.boto.flink.scala.typeutils

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types as JTypes}
import org.apache.flink.types.Row

import scala.jdk.CollectionConverters.*
import scala.util.{Either, Try}

object Types {

  /** Returns type information for [[String]] and [[java.lang.String]]. Supports a null value. */
  implicit val STRING: TypeInformation[String] = JTypes.STRING

  /**
   * Returns type information for primitive [[Byte]] and [[java.lang.Byte]]. Does not support a null
   * value.
   */
  implicit val BYTE: TypeInformation[Byte] = JTypes.BYTE.asInstanceOf[TypeInformation[Byte]]

  /**
   * Returns type information for primitive [[Boolean]] and [[java.lang.Boolean]]. Does not support
   * a null value.
   */
  implicit val BOOLEAN: TypeInformation[Boolean] = JTypes.BOOLEAN.asInstanceOf[TypeInformation[Boolean]]

  /**
   * Returns type information for primitive [[Short]] and [[java.lang.Short]]. Does not support a
   * null value.
   */
  implicit val SHORT: TypeInformation[Short] = JTypes.SHORT.asInstanceOf[TypeInformation[Short]]

  /**
   * Returns type information for primitive [[Int]] and [[java.lang.Integer]]. Does not support a
   * null value.
   */
  implicit val INT: TypeInformation[Int] = JTypes.INT.asInstanceOf[TypeInformation[Int]]

  /**
   * Returns type information for primitive [[Long]] and [[java.lang.Long]]. Does not support a null
   * value.
   */
  implicit val LONG: TypeInformation[Long] = JTypes.LONG.asInstanceOf[TypeInformation[Long]]

  /**
   * Returns type information for primitive [[Float]] and [[java.lang.Float]]. Does not support a
   * null value.
   */
  implicit val FLOAT: TypeInformation[Float] = JTypes.FLOAT.asInstanceOf[TypeInformation[Float]]

  /**
   * Returns type information for primitive [[Double]] and [[java.lang.Double]]. Does not support a
   * null value.
   */
  implicit val DOUBLE: TypeInformation[Double] = JTypes.DOUBLE.asInstanceOf[TypeInformation[Double]]

  /**
   * Returns type information for primitive [[Char]] and [[java.lang.Character]]. Does not support a
   * null value.
   */
  implicit val CHAR: TypeInformation[Char] = JTypes.CHAR.asInstanceOf[TypeInformation[Char]]

}
