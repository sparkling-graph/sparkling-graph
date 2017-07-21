package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.factory

/**
  * Created by mth on 6/25/17.
  */
trait FlowFactory[VD, FD] {
  def create(arg: VD): FD
}
