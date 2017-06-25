package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCFlow(val src: VertexId, val dst: VertexId, val potential: Double, val completed: Boolean, val aliveThrough: Int) extends Serializable {
  def supplyValue(vertexId: VertexId) =
    if (src == vertexId) 1
    else if (dst == vertexId) -1
    else 0

  val key = (src, dst)

  val removable = completed && aliveThrough <= 0

  def countdownVitality = if (aliveThrough > 0) CFBCFlow(src, dst, potential, completed, aliveThrough - 1) else this
}

object CFBCFlow extends Serializable {
  def apply(src: VertexId,
            dst: VertexId,
            potential: Double = 1.0,
            completed: Boolean = false,
            aliveThrough: Int = 3
           ): CFBCFlow = new CFBCFlow(src, dst, potential, completed, aliveThrough)

  def updatePotential(flow: CFBCFlow, newPotential: Double, eps: Double = 0.0) = {
    val completed = Math.abs(flow.potential - newPotential) > eps
    CFBCFlow(flow.src, flow.dst, newPotential, completed, flow.aliveThrough)
  }

  def empty(src: VertexId, dst: VertexId) =
    CFBCFlow(src, dst, 0.0)
}

