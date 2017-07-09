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

  def apply(key: (VertexId, VertexId)) = key match { case (src, dst) => apply(src, dst) }

  def apply(flows: Iterable[CFBCFlow], vertex: CFBCVertex): CFBCNeighbourFlow = {

    def aggregatePotential(vertexFlow: CFBCFlow)(acc: NeighbourFlowStats, flow: CFBCFlow) =
      NeighbourFlowStats.fromFlow(vertexFlow)(flow).merge(acc)

    def mergePotential(acc1: NeighbourFlowStats, acc2: NeighbourFlowStats) = acc1.merge(acc2)

    val (src, dst) = flows.headOption.map(_.key) match { case Some(k) => k }
    val aggregaeFunc = aggregatePotential(vertex.getFlow((src, dst))) _
    val stats = flows.aggregate(NeighbourFlowStats.empty)(aggregaeFunc, mergePotential)
    CFBCNeighbourFlow(src, dst, stats.potential, stats.sumPotentialDiff, flows.size, stats.allCompleted, stats.anyCompleted)
  }

  class NeighbourFlowStats( val potential: Double,
                            val sumPotentialDiff: Double,
                            val allCompleted: Boolean,
                            val anyCompleted: Boolean) extends Serializable {
    def merge(other: NeighbourFlowStats): NeighbourFlowStats = {
      NeighbourFlowStats(
        potential + other.potential,
        sumPotentialDiff + other.sumPotentialDiff,
        allCompleted && other.allCompleted,
        anyCompleted || other.anyCompleted)
    }
  }

  object NeighbourFlowStats extends Serializable {
    def apply(potential: Double, sumPotentialDiff: Double, allCompleted: Boolean, anyCompleted: Boolean): NeighbourFlowStats =
      new NeighbourFlowStats(potential, sumPotentialDiff, allCompleted, anyCompleted)

    def fromFlow(vertexFlow: CFBCFlow)(nbflow: CFBCFlow): NeighbourFlowStats =
      apply(nbflow.potential, Math.abs(nbflow.potential - vertexFlow.potential), nbflow.completed, nbflow.completed)

    def empty = apply(.0, .0, true, false)
  }
}
