package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct

import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages.{BCAggregationMessage, BFSBCConfirmMessage}


/**
  * Created by mth on 5/6/17.
  */
class NOBFSVertex(val distance: Double, val sigma: Int, val psi: Double, val numOfSucc: Int, val receivedAggrResp: Int, val state: Int) extends Serializable {
  def updateBC(p: Double) = {
    NOBFSVertex(distance, sigma, psi + p, numOfSucc, receivedAggrResp + 1, state)
  }

  def updateBC(ps: Iterable[Double]) = {
    NOBFSVertex(distance, sigma, psi + ps.sum, numOfSucc, receivedAggrResp + ps.size, state)
  }

  def setToConfirm =
    NOBFSVertex(distance, sigma, psi, numOfSucc, receivedAggrResp, NOBFSVertex.toConfirm)

  def waitForConfirm =
    NOBFSVertex(distance, sigma, psi, numOfSucc, receivedAggrResp, NOBFSVertex.waitForConfirm)

  def applyConfirmations(confirm: Iterable[BFSBCConfirmMessage]) =
    NOBFSVertex(distance, sigma, psi, numOfSucc + confirm.size, receivedAggrResp, NOBFSVertex.confirmed)

  def updateReceivedAggr(msg: Iterable[BCAggregationMessage]) =
    NOBFSVertex(distance, sigma, psi, numOfSucc, receivedAggrResp + msg.size, state)

  val isCompleted = numOfSucc == receivedAggrResp && state == NOBFSVertex.confirmed
}

object NOBFSVertex extends Serializable {
  def apply(distance: Double, sigma: Int, psi: Double = .0, numOfSucc: Int = 0, receivedAggrResp: Int = 0, state: Int): NOBFSVertex =
    new NOBFSVertex(distance, sigma, psi, numOfSucc, receivedAggrResp, state)

  val idle = 0
  val toConfirm = 1
  val waitForConfirm = 2
  val confirmed = 3
}
