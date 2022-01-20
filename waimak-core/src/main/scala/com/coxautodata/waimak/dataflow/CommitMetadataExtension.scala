package com.coxautodata.waimak.dataflow

import java.util.UUID

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class CommitMetadataExtension[S <: DataFlow[S]](commitMeta: CommitMeta[S]) extends DataFlowMetadataExtension[S] {

  override def preExecutionManipulation(flow: S): S = {

    commitMeta.validate(flow).get

    buildCommits(flow)
      .updateMetadataExtension[CommitMetadataExtension[S]](identifier, _ => None)

  }

  /**
    * During data flow preparation for execution stage, it interacts with data committer to add actions that implement
    * stages of the data committer.
    *
    * This build uses tags to separate the stages of the data committer: cache, move, finish.
    *
    */
  def buildCommits(flow: S): S = {
    commitMeta.pushes.foldLeft(flow) { (resFlow, pushCommitter: (String, Seq[DataCommitter[S]])) =>
      val commitName = pushCommitter._1
      val commitUUID = UUID.randomUUID()
      val committer = pushCommitter._2.head
      val labels = commitMeta.commits(commitName)
      resFlow.tag(commitName) {
        committer.stageToTempFlow(commitName, commitUUID, labels, _)
      }.tagDependency(commitName) {
        _.tag(commitName + "_AFTER_COMMIT") {
          committer.moveToPermanentStorageFlow(commitName, commitUUID, labels, _)
        }
      }.tagDependency(commitName + "_AFTER_COMMIT") {
        committer.finish(commitName, commitUUID, labels, _)
      }
    }
  }

  override def identifier: DataFlowMetadataExtensionIdentifier = CommitMetadataExtensionIdentifier
}

object CommitMetadataExtension {

  import DataFlow._

  /**
    * Whether to cache labels before they are committed if they are reused
    * elsewhere in the flow.
    */
  val CACHE_REUSED_COMMITTED_LABELS: String = s"$dataFlowParamPrefix.cacheReusedCommittedLabels"
  val CACHE_REUSED_COMMITTED_LABELS_DEFAULT: Boolean = true

}

case object CommitMetadataExtensionIdentifier extends DataFlowMetadataExtensionIdentifier

/**
  * Contains configurations for commits and pushes, while configs are added, there are no modifications to the
  * dataflow, as it waits for a validation before execution.
  *
  * @param commits Map[ COMMIT_NAME, Seq[CommitEntry] ]
  * @param pushes  Map[ COMMIT_NAME, Seq[DataCommitter] - there should be one committer per commit name, but due to
  *                lazy definitions of the data flows, validation will have to catch it.
  */
case class CommitMeta[S <: DataFlow[S]](commits: Map[String, Seq[CommitEntry]], pushes: Map[String, Seq[DataCommitter[S]]]) {

  def addCommits(commitName: String, labels: Seq[String], partitions: Option[Either[Seq[String], Int]], repartition: Boolean, cacheLabels: Boolean): CommitMeta[S] = {
    val nextCommits = commits.getOrElse(commitName, Seq.empty) ++ labels.map(CommitEntry(_, commitName, partitions, repartition, cacheLabels))
    this.copy(commits = commits + (commitName -> nextCommits))
  }

  def labelsUsedInMultipleCommits(): Option[Map[String, Seq[String]]] = {
    val labelCommits: Map[String, Seq[String]] = commits.toSeq.flatMap(kv => kv._2.map(c => (c.label, c.commitName))).groupBy(_._1).filter(_._2.size > 1).mapValues(_.map(_._2)).toMap
    Option(labelCommits).filter(_.nonEmpty)
  }

  def addPush(commitName: String, committer: DataCommitter[S]): CommitMeta[S] = {
    val nextPushes = pushes.getOrElse(commitName, Seq.empty) :+ committer
    this.copy(pushes = pushes + (commitName -> nextPushes))
  }

  def pushesWithoutCommits(): Set[String] = pushes.keySet.diff(commits.keySet)

  def commitsWithoutPushes(): Set[String] = commits.keySet.diff(pushes.keySet)

  def validateCommitters(dataFlow: S): Try[Unit] = {

    @tailrec
    def loopTest(pushesToValidate: Set[String], result: Try[Unit]): Try[Unit] = {
      if (pushesToValidate.isEmpty || result.isFailure) result
      else {
        val commit = pushesToValidate.head
        val committers = pushes(commit)
        if (committers.size != 1) Failure(new DataFlowException(s"Commit with name [$commit] has ${committers.size} instead of 1"))
        else loopTest(pushesToValidate.tail, committers.head.validate(dataFlow, commit, commits(commit)))
      }
    }

    loopTest(pushes.keySet.intersect(commits.keySet), Success(()))
  }

  /**
    * Checks if commits refer to labels that are not produced in the flow.
    *
    * @param presentLabels labels that are produced in the data flow
    * @return Map[COMMIT_NAME, Set[Labels that are not defined in the DataFlow, but in the commits] ]
    */
  def phantomLabels(presentLabels: Set[String]): Map[String, Set[String]] = commits.filterKeys(pushes.contains).mapValues(_.map(_.label).toSet.diff(presentLabels)).filter(_._2.nonEmpty).toMap

  def validate(dataFlow: S): Try[Unit] = {
    val outputLabels: Set[String] = dataFlow.inputs.keySet ++ dataFlow.actions.flatMap(_.outputLabels).toSet
    Try {
      val c = commitsWithoutPushes().toArray
      if (c.nonEmpty) throw new DataFlowException(s"There are no push definitions for commits: ${c.sorted.mkString("[", ", ", "]")}")

      val pushes = pushesWithoutCommits().toArray
      if (pushes.nonEmpty) throw new DataFlowException(s"There are no commits definitions for pushes: ${pushes.sorted.mkString("[", ", ", "]")}")

      val notPresent = phantomLabels(outputLabels).mapValues(_.mkString("{", ", ", "}"))
      if (notPresent.nonEmpty) throw new DataFlowException(s"Commit definitions with labels that are not produced by any action: ${notPresent.mkString("[", ", ", "]")}")

    }.flatMap(_ => validateCommitters(dataFlow))
  }

}

object CommitMeta {
  def empty[S <: DataFlow[S]]: CommitMeta[S] = CommitMeta[S](Map.empty, Map.empty)
}

case class CommitEntry(label: String, commitName: String, partitions: Option[Either[Seq[String], Int]], repartition: Boolean, cache: Boolean)