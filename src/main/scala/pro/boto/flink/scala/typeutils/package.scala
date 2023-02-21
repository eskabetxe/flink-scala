package pro.boto.flink.scala

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

package object typeutils {

  export pro.boto.flink.scala.typeutils.Types._

  /**
   * Generates type information based on the given class and/or its type parameters.
   *
   * The definition is similar to a [[org.apache.flink.api.common.typeinfo.TypeHint]] but does not
   * require to implement anonymous classes.
   *
   * If the class could not be analyzed by the Scala type analyzer, the Java analyzer will be used.
   *
   * Example use:
   *
   * `Types.of[(Int, String, String)]` for Scala tuples `Types.of[Unit]` for Scala specific types
   *
   * @tparam T
   * class to be analyzed
   */
//  def typeOf[T: TypeInformation]: TypeInformation[T] = {
//    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
//    typeInfo
//  }

  def typeOf[T](implicit classTag: ClassTag[T]): TypeInformation[T] = {
    TypeInformation.of(classTag.runtimeClass.asInstanceOf[Class[T]])

  }

}
