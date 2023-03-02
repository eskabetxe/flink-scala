package pro.boto.flink.scala.testutils.cluster

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.junit5.MiniClusterExtension
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver

class FlinkClusterExtension extends BeforeAllCallback with BeforeEachCallback with AfterEachCallback
  with AfterAllCallback with ParameterResolver {

  val miniCluster = new MiniClusterExtension(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberTaskManagers(2)
      .setNumberSlotsPerTaskManager(2)
      .build())

  override def beforeAll(context: ExtensionContext): Unit = {
    miniCluster.beforeAll(context)
  }

  override def beforeEach(context: ExtensionContext): Unit = {
    miniCluster.beforeEach(context)
  }

  override def afterEach(context: ExtensionContext): Unit = {
    miniCluster.afterEach(context)
  }

  override def afterAll(context: ExtensionContext): Unit = {
    miniCluster.afterAll(context)
  }

  override def supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean = {
    miniCluster.supportsParameter(parameterContext, extensionContext)
  }

  override def resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): AnyRef = {
    miniCluster.resolveParameter(parameterContext, extensionContext)
  }
}
