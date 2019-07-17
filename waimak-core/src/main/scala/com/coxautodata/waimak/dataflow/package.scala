package com.coxautodata.waimak

import com.coxautodata.waimak.dataflow.CommitExtension._

/**
  * Created by Alexei Perelighin on 2018/01/11.
  */
package object dataflow {

  type ActionResult = Seq[Option[Any]]

  val DEFAULT_POOL_NAME = "DEFAULT_POOL"

  implicit class CommitImplicits[Self <: DataFlow[Self]](flow: Self) {

    /**
      * Groups labels to commit under a commit name.
      * Can be called multiple times with same same commit name, thus adding labels to it.
      * There can be multiple commit names defined in a single data flow.
      *
      * By default, the committer is requested to cache the underlying labels on the flow before writing them out
      * if caching is supported by the data committer. If caching is not supported this parameter is ignored.
      * This behavior can be disabled by setting the [[CACHE_REUSED_COMMITTED_LABELS]] parameter.
      *
      * @param commitName  name of the commit, which will be used to define its push implementation
      * @param partitions  list of partition columns for the labels specified in this commit invocation. It will not
      *                    impact labels from previous or following invocations of the commit with same commit name.
      * @param repartition to repartition the data
      * @param labels      labels added to the commit name with partitions config
      * @return
      */
    def commit(commitName: String, partitions: Seq[String], repartition: Boolean = true)(labels: String*): Self = {
      commit(commitName, Some(Left(partitions)), repartition = partitions.nonEmpty && repartition)(labels: _*)
    }

    /**
      * Groups labels to commit under a commit name.
      * Can be called multiple times with same same commit name, thus adding labels to it.
      * There can be multiple commit names defined in a single data flow.
      *
      * By default, the committer is requested to cache the underlying labels on the flow before writing them out
      * if caching is supported by the data committer. If caching is not supported this parameter is ignored.
      * This behavior can be disabled by setting the [[CACHE_REUSED_COMMITTED_LABELS]] parameter.
      *
      * @param commitName  name of the commit, which will be used to define its push implementation
      * @param repartition how many partitions to repartition the data by
      * @param labels      labels added to the commit name with partitions config
      * @return
      */
    def commit(commitName: String, repartition: Int)(labels: String*): Self = {
      commit(commitName, Some(Right(repartition)), repartition = true)(labels: _*)
    }

    /**
      * Groups labels to commit under a commit name.
      * Can be called multiple times with same same commit name, thus adding labels to it.
      * There can be multiple commit names defined in a single data flow.
      *
      * By default, the committer is requested to cache the underlying labels on the flow before writing them out
      * if caching is supported by the data committer. If caching is not supported this parameter is ignored.
      * This behavior can be disabled by setting the [[CACHE_REUSED_COMMITTED_LABELS]] parameter.
      *
      * @param commitName name of the commit, which will be used to define its push implementation
      * @param labels     labels added to the commit name with partitions config
      * @return
      */
    def commit(commitName: String)(labels: String*): Self = {
      commit(commitName, None, repartition = false)(labels: _*)
    }

    private def commit(commitName: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean)(labels: String*): Self = {
      val cacheLabels = flow.flowContext.getBoolean(CACHE_REUSED_COMMITTED_LABELS, CACHE_REUSED_COMMITTED_LABELS_DEFAULT)

      updateCommitMeta(_.addCommits(commitName, labels, partitions, repartition, cacheLabels))
    }


    /**
      * Associates commit name with an implementation of a data committer. There must be only one data committer per one commit name.
      *
      * @param commitName
      * @param committer
      * @return
      */
    def push(commitName: String)(committer: DataCommitter[Self]): Self = updateCommitMeta(_.addPush(commitName, committer))

    private def updateCommitMeta(update: CommitMeta[Self] => CommitMeta[Self]): Self =
      flow.updateExtensionMetadata(CommitExtension[Self], m => update(m.getMetadataAsType[CommitMeta[Self]]))

  }

}
