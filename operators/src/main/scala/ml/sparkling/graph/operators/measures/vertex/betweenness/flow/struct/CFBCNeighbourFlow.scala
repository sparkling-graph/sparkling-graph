package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/2/17.
  */
class CFBCNeighbourFlow(
                         val src: VertexId,
                         val dst: VertexId,
                         val sumOfPotential: Double,
                         val sumOfDifferences: Double,
                         val numberOfFlows: Int,
                         val allCompleted: Boolean,
                         val anyCompleted: Boolean) extends Serializable {

  val key = (src, dst)
}

object CFBCNeighbourFlow extends Serializable {
  def apply(src: VertexId,
            dst: VertexId,
            sumOfPotential: Double = .0,
            sumOfDifferences: Double = .0,
            numberOfFlows: Int = 0,
            allCompleted: Boolean = true,
            anyCompleted: Boolean = true
           ): CFBCNeighbourFlow = new CFBCNeighbourFlow(src, dst, sumOfPotential, sumOfDifferences, numberOfFlows, allCompleted, anyCompleted)

  def apply(flows: Iterable[CFBCFlow], vertex: CFBCVertex): CFBCNeighbourFlow = {

    def aggregatePotential(vertexFlow: CFBCFlow)(acc: (Double, Double, Boolean, Boolean), flow: CFBCFlow) =
      (acc._1 + flow.potential, acc._2 + Math.abs(flow.potential - vertexFlow.potential), acc._3 && flow.completed, acc._4 || flow.completed)

    def mergePotential(acc1: (Double, Double, Boolean, Boolean), acc2: (Double, Double, Boolean, Boolean)) =
      (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 && acc2._3, acc1._4 || acc2._4)

    val (src, dst) = flows.head.key
    val aggregaeFunc = aggregatePotential(vertex.getFlow((src, dst))) _
    val (sum, diff, allComp, anyComp) = flows.aggregate((0.0, 0.0, true, false))(aggregaeFunc, mergePotential)
    CFBCNeighbourFlow(src, dst, sum, diff, flows.size, allComp, anyComp)
  }
}
