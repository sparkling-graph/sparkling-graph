package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator

/**
  * Created by mth on 4/28/17.
  */
trait FlowGenerator[SR, RS] extends Serializable {
  def flowsPerVertex: Int
  def createFlow(arg: SR): RS
  val phi: Int
}
