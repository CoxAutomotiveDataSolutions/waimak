package com.coxautodata.waimak.configuration

import java.util.Properties

import com.coxautodata.waimak.dataflow.spark.SparkFlowContext
import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.RuntimeConfig

import scala.annotation.StaticAnnotation
import scala.util.Try

object CaseClassConfigParser extends Logging {

  val configParamPrefix: String = "spark.waimak.config"

  /**
    * Prefix to add to parameters when looking in the Spark conf. For example, if looking for parameter
    * `args.arg1` then the parser will look for `spark.args.arg1` in the Spark conf by default.
    * This can be disabled by setting this property to an empty string.
    */
  val SPARK_CONF_PROPERTY_PREFIX: String = s"$configParamPrefix.sparkConfPropertyPrefix"
  val SPARK_CONF_PROPERTY_PREFIX_DEFAULT: String = "spark."
  /**
    * Comma separated list of property provider builder object names to instantiate.
    * Set this to have the config parser use the custom objects to search for configuration
    * parameter values in these property providers.
    */
  val CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES: String = s"$configParamPrefix.propertyProviderBuilderObjects"
  val CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES_DEFAULT: List[String] = List.empty
  /**
    * URI of the properties file used by the [[PropertiesFilePropertyProviderBuilder]] object.
    * Used when [[CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES]] includes [[PropertiesFilePropertyProviderBuilder]].
    * File will be opened using an Hadoop FileSystem object, therefore URI must be supported by your
    * Hadoop libraries and configuration must be present in the HadoopConfiguration on the SparkSession.
    */
  val CONFIG_PROPERTIES_FILE_URI: String = s"$configParamPrefix.propertiesFileURI"

  final case class separator(s: String) extends StaticAnnotation

  import reflect.runtime.{currentMirror => cm, universe => ru}
  import ru.typeOf
  import scala.reflect.runtime.universe.{TypeTag, symbolOf}

  /**
    * Cast a String to a particular type representation
    *
    * @param value         The String representation taken from the SparkConf
    * @param typeSignature The type to cast the String to
    * @return String value casted to a given type
    */
  @throws(classOf[UnsupportedOperationException])
  @throws(classOf[NumberFormatException])
  @throws(classOf[IllegalArgumentException])
  private def castAs(value: String, typeSignature: ru.Type): Any = typeSignature match {
    // Theoretically we could check the presence of Type.valueOf(String) or Type.apply(String) using reflection
    // and attempt to call these too for the type conversion
    case t if t =:= typeOf[String] => value
    case t if t =:= typeOf[Byte] => value.toByte
    case t if t =:= typeOf[Short] => value.toShort
    case t if t =:= typeOf[Int] => value.toInt
    case t if t =:= typeOf[Long] => value.toLong
    case t if t =:= typeOf[Float] => value.toFloat
    case t if t =:= typeOf[Double] => value.toDouble
    case t if t =:= typeOf[Boolean] => value.toBoolean
    case x => throw new UnsupportedOperationException(s"Cannot handle conversion of SparkConf value String to ${x.toString}")
  }

  /**
    * Get the value from a separator annotation a parameter might have.
    * Default is returned if annotation is not present.
    *
    * @param param   Parameter
    * @param default Default value if missing
    * @return Separator
    */
  private def getSeparator(param: ru.Symbol, default: String = ","): String = {
    param.annotations.find(_.tree.tpe =:= ru.typeOf[separator])
      .flatMap(_.tree.children.tail.collectFirst { case ru.Literal(ru.Constant(s: String)) => s })
      .getOrElse(default)
  }

  /**
    * Look for the value of a key, searching in SparkConf and then in Properties if it is defined
    *
    * @param conf       SparkConf containing spark configuration
    * @param properties Optional properties configuration
    * @param prefix     Parameter prefix
    * @param param      Parameter name
    * @return String value of parameter
    */
  @throws(classOf[NoSuchElementException])
  private def getValue(conf: Map[String, String], properties: Seq[PropertyProvider], prefix: String, param: String): String = {
    val fullParam = prefix + param
    conf
      .getOrElse(fullParam, properties.toStream.map(p => p.get(fullParam)).collectFirst { case Some(p) => p }.getOrElse {
        throw new NoSuchElementException
      })
  }

  /**
    * Get a parameter from an instance of SparkConf and cast to a given type.
    * Key in SparkConf will be ${prefix}${param.name}.
    *
    * @param conf   Instance of SparkConf containing KeyValue configuration
    * @param prefix Prefix to assign to a Key when looking in SparkConf
    * @param param  Parameter to look for. Name of parameter will form key, and type will be the casted type
    * @return Parameter value casted to a given type
    */
  @throws(classOf[NoSuchElementException])
  @throws(classOf[UnsupportedOperationException])
  @throws(classOf[NumberFormatException])
  @throws(classOf[IllegalArgumentException])
  private def getParam(conf: Map[String, String], prefix: String, param: ru.Symbol, properties: Seq[PropertyProvider]): Any = {
    param.typeSignature match {
      // Option types cast as the inner type of Option
      case t if t <:< typeOf[Option[_]] => Some(getValue(conf, properties, prefix, param.name.toString)).map { v =>
        castAs(v, param.typeSignature.typeArgs.head)
      }
      // Sequence types split by separator and cast to correct type
      case t if t <:< typeOf[Seq[_]] =>
        val sep = getSeparator(param)
        val arr = getValue(conf, properties, prefix, param.name.toString).split(sep)
        val casted = {
          // Important to match types as they become more generic
          t match {
            case l if l <:< typeOf[List[_]] => arr.toList
            case l if l <:< typeOf[Vector[_]] => arr.toVector
            case l if l <:< typeOf[Seq[_]] => arr.toSeq
            case e => throw new UnsupportedOperationException(s"Cannot handle collection type ${e.toString}")
          }
        }
          .map { v =>
            castAs(v, param.typeSignature.typeArgs.head)
          }
        casted
      case x =>
        val res = getValue(conf, properties, prefix, param.name.toString)
        castAs(res, param.typeSignature)
    }
  }

  private def getPropertyProviders(context: SparkFlowContext): Seq[PropertyProvider] = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    context
      .getStringList(CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES, CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES_DEFAULT)
      .map(m.staticModule)
      .map(m.reflectModule)
      .map(_.instance.asInstanceOf[PropertyProviderBuilder])
      .map(_.getPropertyProvider(context))
  }

  def getStrippedSparkProperties(conf: RuntimeConfig, prefix: String): Map[String, String] = conf.getAll.collect {
    case (k, v) if k.startsWith(prefix) => k.stripPrefix(prefix) -> v
  }

  /**
    * Populate a Case Class from an instance of SparkConf. It will attempt to cast the
    * configuration values to the correct types, and most primitive, Option[primitive],
    * and List/Seq/Vector[primitive] types are supported.
    * Default arguments in the case class will also be respected.
    * Option types will not be set to None if they are not specified in the SparkConf unless None
    * is the default value in the case class.
    * The separator for collection types (List,Seq,Vector) is by default "," but can be changed with
    * '@separator("..")' annotation on the parameter in the case class.
    * The parameters keys that are looked up will be of the form: {prefix}{parameter},
    * e.g. for case class Ex(key: String) and prefix="example.prefix.",
    * then the key will have the form "example.prefix.key"
    * By default, properties in the SparkConf will be looked up with an additional property (see [[SPARK_CONF_PROPERTY_PREFIX]]).
    *
    * @param context        Instance of [[SparkFlowContext]] containing a spark session with configuration
    * @param prefix         Prefix to assign to a Key when looking in SparkConf
    * @param additionalConf An additional set of properties to search. Preference is given to SparkConf values if the key exists in
    *                       both this additionalConf and SparkConf
    * @tparam A Case class type to construct
    * @return An instantiated case class populated from the SparkConf instance and default arguments
    */
  @throws(classOf[NoSuchElementException])
  @throws(classOf[UnsupportedOperationException])
  @throws(classOf[NumberFormatException])
  @throws(classOf[IllegalArgumentException])
  def apply[A: TypeTag](context: SparkFlowContext, prefix: String, additionalConf: Map[String, String] = Map.empty): A = {
    val fromSparkConf = getStrippedSparkProperties(context.spark.conf, context.getString(SPARK_CONF_PROPERTY_PREFIX, SPARK_CONF_PROPERTY_PREFIX_DEFAULT))
    fromMap[A](additionalConf ++ fromSparkConf, prefix, getPropertyProviders(context))
  }

  def fromMap[A: TypeTag](conf: Map[String, String], prefix: String = "", properties: Seq[PropertyProvider] = Seq.empty): A = {
    val tag = implicitly[TypeTag[A]]
    val runtimeClass = tag.mirror.runtimeClass(tag.tpe)
    val classSymbol = symbolOf[A].asClass
    val constructorParams = classSymbol.primaryConstructor.asMethod.paramLists.head
    val companionSymbol = classSymbol.companion
    val companionApply = companionSymbol.typeSignature.member(ru.TermName("apply")).asMethod
    val im = Try(cm.reflect(cm.reflectModule(companionSymbol.asModule).instance)).recover {
      case e: ScalaReflectionException => throw new UnsupportedOperationException(s"ScalaReflectionException was thrown when " +
        s"inspecting case class: ${runtimeClass.getSimpleName}. This was likely due to the case class being defined " +
        s"within a class. Definition within a class is not supported.", e)
      case e => throw e
    }.get
    val args = constructorParams.zipWithIndex.map { case (p, i) =>
      Try(getParam(conf, prefix, p, properties))
        .recover {
          case e: NoSuchElementException =>
            val defarg = companionSymbol.typeSignature.member(ru.TermName(s"apply$$default$$${i + 1}"))
            if (!defarg.isMethod) throw new NoSuchElementException(s"No SparkConf configuration value, no value in any PropertyProviders or default " +
              s"value found for parameter $prefix${p.name.toString}")
            else im.reflectMethod(defarg.asMethod)()
          case e => throw e
        }.get
    }
    im.reflectMethod(companionApply)(args: _*).asInstanceOf[A]
  }

}

/**
  * Trait used to define an object that constructs a [[PropertyProvider]].
  * You should extend this trait with an object to define a custom
  * property provider builder. This object can then be used to provide a
  * custom method of retrieving configuration by setting the
  * [[CaseClassConfigParser.CONFIG_PROPERTY_PROVIDER_BUILDER_MODULES]] property
  * on the SparkSession.
  */
trait PropertyProviderBuilder {
  def getPropertyProvider(conf: SparkFlowContext): PropertyProvider
}

/**
  * Trait used to defined a class/object that is used to retrieve
  * configuration parameters.
  * A subtype of this trait is created by the [[PropertyProviderBuilder.getPropertyProvider]]
  * function and not directly instantiated.
  */
trait PropertyProvider {

  /**
    * Get a given configuration property value given a key.
    * Should return [[None]] of the property does not exist.
    */
  def get(key: String): Option[String]
}

/**
  * A property provider implementation that simply wraps around a [[java.util.Properties]] object
  */
class JavaPropertiesPropertyProvider(properties: Properties) extends PropertyProvider {
  override def get(key: String): Option[String] = Option(properties.getProperty(key))
}