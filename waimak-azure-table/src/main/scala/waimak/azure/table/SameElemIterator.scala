package waimak.azure.table

import scala.collection.mutable.ListBuffer

/**
  * Converts an iterator of records that are already sorted by a key into an iterator of pages of records with the same key.
  *
  * Created by Alexei Perelighin on  2017/12/18
  *
  * @param it     iterator of pre sorted records
  * @param isSame function that can extract and compare keys of the 2 records. Returns true if records are have same key
  * @tparam E data type of the records in the iterator
  */
class SameElemIterator[E](it: BufferedIterator[E], maxPageSize: Int, isSame: (E, E) => Boolean) extends Iterator[Seq[E]] {

  override def hasNext: Boolean = it.hasNext

  override def next(): Seq[E] = {
    val buffer = new ListBuffer[E]()
    val sample = it.next()
    buffer += sample
    var toContinue = true
    while (toContinue && buffer.size < maxPageSize && it.hasNext) {
      toContinue = isSame(sample, it.head)
      if (toContinue) {
        buffer += it.next()
      }
    }
    buffer
  }
}