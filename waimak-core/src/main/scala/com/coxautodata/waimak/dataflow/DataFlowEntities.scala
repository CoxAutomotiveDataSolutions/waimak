package com.coxautodata.waimak.dataflow

/**
  * Maintains data flow entities (the inputs and outputs of data flow actions). Every entity has a label which must be
  * unique across the data flow.
  *
  * @param entities a map of label -> entity
  * @tparam T the entity type (e.g. org.apache.spark.sql.Dataset)
  */
//TODO: Make values optional
class DataFlowEntities[T](val entities: Map[String, T]) {

  def filterLabels(labels: List[String]): DataFlowEntities[T] = DataFlowEntities(entities.filterKeys(labels.contains))

  def get(label: String): T = entities(label)

  def ++(other: DataFlowEntities[T]): DataFlowEntities[T] = DataFlowEntities(entities ++ other.entities)

  def +(entity: (String, T)): DataFlowEntities[T] = DataFlowEntities(entities + entity)

  def size: Int = entities.size

  def labels: Set[String] = entities.keySet

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataFlowEntities[T]]

  def map[A](f: T => A) = DataFlowEntities(entities.mapValues(f))

  def filterValues(cond: T => Boolean): DataFlowEntities[T] = DataFlowEntities(entities.filter {
    case (_, entity) => cond(entity)
  })

  def nonEmpty: Boolean = entities.nonEmpty

  def isEmpty: Boolean = entities.isEmpty

  def getAll: Seq[T] = entities.values.toSeq

  override def equals(other: Any): Boolean = other match {
    case that: DataFlowEntities[T] =>
      (that canEqual this) &&
        hash == that.hash &&
        entities == that.entities
    case _ => false
  }

  lazy val hash: Int = {
    val state = Seq(entities)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def hashCode(): Int = hash
}

object DataFlowEntities {

  def apply[T](entities: Map[String, T]): DataFlowEntities[T] = new DataFlowEntities[T](entities)

  def empty[T] = new DataFlowEntities[T](Map.empty)

}
