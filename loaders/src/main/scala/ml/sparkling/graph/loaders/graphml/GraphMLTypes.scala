package ml.sparkling.graph.loaders.graphml

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphMLTypes {


  trait TypeHandler extends Serializable{
    def apply(value:String):Any
  }
 def apply(typeName:String):TypeHandler={
   val typeHandlerProvider = Map[String,TypeHandler]("string" -> StringHanlder,"boolean" -> BooleanHandler, "int" -> IntHandler )
   typeHandlerProvider(typeName.toLowerCase)
 }
  object StringHanlder extends TypeHandler{
    override def apply(value: String): String = value
  }

  object BooleanHandler extends TypeHandler{
    override def apply(value: String): Boolean = value.toBoolean
  }

  object IntHandler extends TypeHandler{
    override def apply(value: String): Int = value.toInt
  }

  object LongHandler extends TypeHandler{
    override def apply(value: String): Long = value.toLong
  }

  object FloatHandler extends TypeHandler{
    override def apply(value: String): Float = value.toFloat
  }

  object DoubleHandler extends TypeHandler{
    override def apply(value: String): Double = value.toDouble
  }
}
