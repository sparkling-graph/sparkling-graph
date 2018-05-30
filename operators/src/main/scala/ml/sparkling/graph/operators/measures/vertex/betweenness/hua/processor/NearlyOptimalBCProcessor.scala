package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.processor

import ml.sparkling.graph.operators.algorithms.bfs.BFSShortestPath
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.predicate.NOInitBFSPredicate
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.{NOBFSVertex, NOVertex}
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages._
import org.apache.spark.graphx._

import scala.Array._
import scala.reflect.ClassTag

/**
  * Created by mth on 5/7/17.
  */
class NearlyOptimalBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  val initVertexId = graph.ops.pickRandomVertex()

  lazy val initGraph = initBFS

  private def initBFS = {
    val preparedGraph = graph.mapVertices((id, _) => NOVertex(id))
    val initBFSProcessor = new BFSShortestPath[NOVertex, ED, List[NOMessage[VertexId]]](new NOInitBFSPredicate, new NOInitBFSProcessor[ED]())
    initBFSProcessor.computeSingleSelectedSourceBFS(preparedGraph, initVertexId)
  }

  def prepareVertices(startVertex: VertexId)(vertex: NOVertex) = vertex.vertexId match {
    case vId if startVertex == vId =>
      val nextVert = vertex.lowestSucc
      val pointer = Some(DFSPointer(startVertex, nextVert, toSent = true))
      val succ = updateSuccSet(vertex, pointer)
      vertex.update(succ = succ, dfsPointer = pointer, bfsMap = Map(startVertex -> NOBFSVertex(.0, 1, state = NOBFSVertex.toConfirm)))
    case _ => vertex
  }

  def applyMessages(round: Int)(vertexId: VertexId, vertex: NOVertex, messages: Option[List[NOMessage[VertexId]]]) = {
    val msg = messages.getOrElse(List.empty)
    val pointer = msg.filter(_.isDFSPointer).map(_.asInstanceOf[DFSPointer])
    val bfsMsg = msg.filter(m => m.isExpand || m.isConfirm || m.isAggregation)

    val newPointer = updateDFSPointer(vertex, pointer.headOption)
    val newSucc = updateSuccSet(vertex, newPointer)
    val newBfsMap = updateBfsMap(vertexId, vertex.bfsMap, bfsMsg)

    val toRemove = newBfsMap.filter({ case (key, value) => value.isCompleted && key != vertexId })

    val bcIncrees = toRemove.values.map(n => n.psi * n.sigma.toDouble).sum

    newPointer match {
      case Some(ptr) if !ptr.toSent =>
        val newBfs = (vertexId, NOBFSVertex(.0, 1, .0, 0, 0, NOBFSVertex.idle))
        val bfsMap = newBfsMap + newBfs
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = bfsMap, bcInc = bcIncrees)
      case _ =>
        vertex.update(succ = newSucc, dfsPointer = newPointer, bfsMap = newBfsMap, bcInc = bcIncrees)
    }
  }

  def updateDFSPointer(vertex: NOVertex, pointerMsg: Option[DFSPointer]): Option[DFSPointer] =
    vertex.dfsPointer match {
      case Some(pointer) if pointer.toRemove => None
      case Some(pointer) if !vertex.leaf => Some(pointer.asToSent())
      case Some(pointer) if vertex.leaf => Some(pointer.asReturning)
      case None => pointerMsg match {
        case Some(pointer) if pointer.returning && vertex.leaf => pointerMsg
        case Some(pointer) if pointer.returning && !vertex.leaf => Some(pointer.asToSent(vertex.lowestSucc))
        case Some(pointer) => Some(pointer.asWaiting(vertex.lowestSucc))
        case _ => None
      }
    }

  def updateSuccSet(vertex: NOVertex, pointer: Option[DFSPointer]): Option[Array[VertexId]] = pointer match {
    case Some(p) if p.next.nonEmpty && vertex.succ.nonEmpty =>
      Some(vertex.succ.getOrElse(empty[VertexId]).filterNot(it => p.next.exists(_ == it)))
    case _ => vertex.succ
  }

  def updateBfsMap(vertexId: VertexId, map: Map[VertexId, NOBFSVertex], messages: List[NOMessage[VertexId]]) = {
    val msgMap = messages.groupBy(_.source)

    val filteredFlowsMap = map.filter({ case (root, flow) => !flow.isCompleted})
    val expandMessages = messages.filter(_.isExpand).groupBy(_.source)

    val msgVertex2 = filteredFlowsMap.map({ case (root, flow) =>
      val messages = msgMap.getOrElse(root, List.empty)
      flow.state match {
        case NOBFSVertex.idle =>
          (root, flow.setToConfirm)
        case NOBFSVertex.toConfirm if messages.nonEmpty =>
          throw new Error("Unsuspected messages is state toConfirm")
        case NOBFSVertex.toConfirm =>
          (root, flow.waitForConfirm)
        case NOBFSVertex.waitForConfirm if messages.exists(m => m.isAggregation || m.isExpand) =>
          throw new Error("Unsuspected messages is state waitForConfirm")
        case NOBFSVertex.waitForConfirm =>
          val confirmations = messages.map(_.asInstanceOf[BFSBCConfirmMessage])
          (root, flow.applyConfirmations(confirmations))
        case NOBFSVertex.confirmed if messages.exists(m => m.isExpand || m.isConfirm) =>
          throw new Error("Unsuspected messages is state confirmed")
        case NOBFSVertex.confirmed =>
          val aggregations = messages.map(_.asInstanceOf[BCAggregationMessage].psi)
          (root, flow.updateBC(aggregations))
        case _ => throw new Error("Unsuspected case")
      }
    })

    val createdFlows = expandMessages.map({
      case (root, expand: List[BFSBCExtendMessage @unchecked]) =>
      if (msgVertex2.contains(root)) throw new Error("Attempt to create duplicate of vertex")
      val sigma = expand.map(_.sigma).sum
      val distance = expand.headOption.map(_.distance).getOrElse(.0)
      val vertex = NOBFSVertex(distance, sigma, state = NOBFSVertex.toConfirm)
      (root, vertex)
    })

    msgVertex2 ++ createdFlows
  }

  def sendMessages(round: Int)(ctx: EdgeContext[NOVertex, ED, List[NOMessage[VertexId]]]): Unit = {
    val triplet = ctx.toEdgeTriplet
    val pointerSender = sendPointer(triplet) _
    pointerSender(ctx.srcId, ctx.sendToSrc)
    pointerSender(ctx.dstId, ctx.sendToDst)

    val extSender = sendBFSExtendMessage(triplet) _
    extSender(ctx.srcId, ctx.sendToSrc)
    extSender(ctx.dstId, ctx.sendToDst)

    val confirmSender = sendConfirmation(triplet) _
    confirmSender(ctx.srcId, ctx.sendToSrc)
    confirmSender(ctx.dstId, ctx.sendToDst)

    val aggregationSender = sendAggregate(triplet) _
    aggregationSender(ctx.srcId, ctx.sendToSrc)
    aggregationSender(ctx.dstId, ctx.sendToDst)
  }

  private def sendPointer(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
    val srcAttr = triplet.otherVertexAttr(dst)
    srcAttr.dfsPointer match {
      case Some(pointer) if pointer.returning && pointer.toSent && srcAttr.pred.exists(_ == dst) => send(List(pointer))
      case Some(pointer) if pointer.toSent && pointer.next.exists(_ == dst) => send(List(pointer))
      case _ =>
    }
  }

  private def sendBFSExtendMessage(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
    val srcAttr = triplet.otherVertexAttr(dst)
    val dstAttr = triplet.vertexAttr(dst)
    srcAttr.bfsMap.foreach({ case (root, vertex) =>
      if (!dstAttr.bfsMap.contains(root) && vertex.state == NOBFSVertex.toConfirm)
        send(List(BFSBCExtendMessage.create(root, vertex)))
    })
  }

  private def sendConfirmation(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
    val srcAttr = triplet.otherVertexAttr(dst)
    val dstAttr = triplet.vertexAttr(dst)
    srcAttr.bfsMap.filter({ case (key, v) => v.state == NOBFSVertex.toConfirm })
      .foreach({ case (key, v) =>
        dstAttr.bfsMap.get(key) match {
          case Some(parent) if isParentWaitingForConfirm(v, parent) =>
            send(List(BFSBCConfirmMessage(key)))
          case _ =>
        }
      })
  }

  private def isParentWaitingForConfirm(vert: NOBFSVertex, parent: NOBFSVertex) =
    isParent(vert, parent) && parent.state == NOBFSVertex.waitForConfirm

  private def isParent(vert: NOBFSVertex, parent: NOBFSVertex) = vert.distance == parent.distance + 1

  private def sendAggregate(triplet: EdgeTriplet[NOVertex, ED])(dst: VertexId, send: (List[NOMessage[VertexId]]) => Unit) = {
    val srcAttr = triplet.otherVertexAttr(dst)
    val dstAttr = triplet.vertexAttr(dst)
    srcAttr.bfsMap.filter({ case (key, v) => dstAttr.bfsMap.get(key).exists(p => isParent(v, p)) && v.isCompleted})
      .foreach({ case (key, v) =>
        send(List(BCAggregationMessage(key, 1.0 / v.sigma.toDouble + v.psi)))
      })
  }
}
