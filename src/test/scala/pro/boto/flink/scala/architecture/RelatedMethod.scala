package pro.boto.flink.scala.architecture

import scala.reflect.ClassTag


case class RelatedMethod(name: String, retClass: String, paramClass: String*) {
  override def toString: String = s"$retClass $name(${paramClass.mkString(", ")})"
}

object RelatedMethod {
  def classFrom[T](implicit classTag: ClassTag[T]): String = classFrom[T]("")

  def classFrom[T, P](implicit classTag: ClassTag[T], paramTag: ClassTag[P]): String = classFrom[T](paramTag.runtimeClass.getName)

  def classFrom[T](param: String, params: String*)(implicit classTag: ClassTag[T]): String = s"${classTag.runtimeClass.getName}${if (param.isEmpty) "" else s"<${(List(param) ++ params).mkString(", ")}>"}"

  def extendsFrom[T](implicit classTag: ClassTag[T]): String = extendsFrom[T]("")
  def extendsFrom[T](param: String, params: String*)(implicit classTag: ClassTag[T]): String = s"? extends ${classFrom[T](param, params:_*)}"

  def arrayFrom[T](implicit classTag: ClassTag[T]): String = arrayFrom[T]("")

  def arrayFrom[T](param: String, params: String*)(implicit classTag: ClassTag[T]): String = s"${classFrom[T](param, params: _*)}[]"
}
