package pro.boto.flink.scala.architecture

import com.tngtech.archunit.base.DescribedPredicate
import com.tngtech.archunit.base.HasDescription
import com.tngtech.archunit.core.domain._
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang._
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods
import org.apache.flink.annotation.Experimental
import org.apache.flink.annotation.Internal

import scala.jdk.CollectionConverters._

object ClassMethods {
  private def methodToRelated(method: JavaMethod): RelatedMethod = {
    val name = method.getName
    val retClass = method.getReturnType.getName
    val paramClass = method.getParameterTypes.asScala.map(_.getName).toList
    RelatedMethod(name, retClass, paramClass: _*)
  }

  private def methodFilter(knowMethodsName: Set[String]) = new DescribedPredicate[JavaMethod](s"have name in (${knowMethodsName.mkString(", ")})") {
    def test(input: JavaMethod): Boolean = knowMethodsName.contains(input.getName)
  }

  private def methodChecker(knowMethods: List[RelatedMethod]) = new ArchCondition[JavaMethod]("be a well-known method") {
    def check(javaMethod: JavaMethod, events: ConditionEvents): Unit = {
      val relatedMethod = methodToRelated(javaMethod)
      val retVal = knowMethods.contains(relatedMethod)
      val message = s"${if (!retVal) "not " else ""}found: $relatedMethod"
      events.add(new SimpleConditionEvent(javaMethod, retVal, message))
    }
  }

  private def methodsToCheck() = methods()
    .that().arePublic()
    .and().areNotAnnotatedWith(classOf[Internal])
    .and().areNotAnnotatedWith(classOf[Deprecated])
    .and().areNotAnnotatedWith(classOf[Experimental])

  def checkKnownMethod(theClass: Class[?], knowMethods: List[RelatedMethod]): EvaluationResult = {
    methodsToCheck()
      .and(methodFilter(knowMethods.map(_.name).toSet))
      .should(methodChecker(knowMethods))
      .evaluate(new ClassFileImporter().importClasses(theClass))
  }

  def checkKnownMethodExist(theClass: Class[?], knowMethods: List[RelatedMethod]): EvaluationResult = {
    var checkMethods = knowMethods
    val checkEvaluation = methodsToCheck()
      .and(methodFilter(checkMethods.map(_.name).toSet))
      .should(new ArchCondition[JavaMethod](s"exist on ${theClass.getName}") {
        def check(javaMethod: JavaMethod, events: ConditionEvents): Unit = {
          val relatedMethod = methodToRelated(javaMethod)
          val retVal = checkMethods.contains(relatedMethod)
          if (retVal) {
            checkMethods = checkMethods.filterNot(m => m == relatedMethod)
          }
          val message = s"${if (!retVal) "not " else ""}found: $relatedMethod"
          events.add(new SimpleConditionEvent(javaMethod, retVal, message))
        }
      })
      .evaluate(new ClassFileImporter().importClasses(theClass))
    if (checkMethods.nonEmpty) {
      checkEvaluation.add(
        new EvaluationResult(
          new HasDescription() {
            def getDescription: String = "not found method"
          },
          {
            val error = ConditionEvents.Factory.create()
            error.add(new SimpleConditionEvent(theClass, checkMethods.isEmpty, checkMethods.map(_.toString).mkString("\n")))
            error
          },
          checkEvaluation.getPriority
        )
      )
    }
    checkEvaluation

  }

  def checkClassMethod(theClass: Class[?], knowMethods: List[RelatedMethod]): EvaluationResult = {
    methodsToCheck()
      .should(methodChecker(knowMethods))
      .evaluate(new ClassFileImporter().importClasses(theClass))
  }

}

