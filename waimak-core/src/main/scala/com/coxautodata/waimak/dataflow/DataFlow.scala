package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging
import org.apache.spark.sql.Dataset

import scala.util.{Failure, Success, Try}

/**
  * Defines a state of the data flow. State is defined by the inputs that are ready to be consumed and actions that need
  * to be executed.
  * In most of the BAU cases, initial state of the data flow has no inputs, as they need to be produced by the actions.
  * When an action finishes, it can produce 0 or N outputs, to create next state of the data flow, that action is removed
  * from the data flow and its outputs are added as inputs into the flow. This state transitioning will enable restarts
  * of the flow from any point or debug/exploratory runs with already existing/manufactured/captured/materialised inputs.
  *
  * Also inputs are useful for unit testing, as they give access to all intermediate outputs of actions.
  *
  * @tparam T the type of the entity which we are transforming (e.g. Dataset
  * @tparam C the type of context which we pass to the actions
  */
trait DataFlow[T, C] extends Logging {

  val flowContext: C

  /**
    * Inputs that were explicitly set or produced by previous actions, these are inputs for all following actions.
    * Inputs are preserved in the data flow state, even if they are no longer required by the remaining actions.
    * //TODO: explore the option of removing the inputs that are no longer required by remaining actions!!!
    *
    * @return
    */
  def inputs: DataFlowEntities[Option[T]]

  /**
    * Actions to execute, these will be scheduled when inputs become available. Executed actions must be removed from
    * the sate.
    *
    * @return
    */
  def actions: Seq[DataFlowAction[T, C]]

  def tagState: DataFlowTagState

  /**
    * Creates new state of the dataflow by adding an action to it.
    *
    * @param action - action to add
    * @return - new state with action
    * @throws  DataFlowException when:
    *                            1) at least one of the input labels is not present in the inputs
    *                            2) at least one of the input labels is not present in the outputs of existing actions
    */
  def addAction(action: DataFlowAction[T, C]): this.type = {

    action.outputLabels.foreach(l => labelIsInputOrProduced(l) match {
      case true => throw new DataFlowException(s"Output label [${l}] is already in the inputs or is produced by another action.")
      case _ => Unit
    })
    val newActions = actions :+ action
    // Add current action into tagstate with current active tags/dep tags
    val newTagState = tagState.copy(taggedActions = tagState.taggedActions + (action.guid -> DataFlowActionTags(tagState.activeTags, tagState.activeDependentOnTags)))
    createInstance(inputs, newActions, newTagState).asInstanceOf[this.type]
  }

  /**
    * Transforms the current dataflow by applying a function to it.
    *
    * @param f A function that transforms a dataflow object
    * @return New dataflow
    */
  def map[R >: this.type](f: this.type => R): R = f(this)

  /**
    * Optionally transform a dataflow depending on the output of the
    * applying function. If the transforming function returns a None
    * then the original dataflow is returned.
    *
    * @param f A function that returns an Option[DataFlow]
    * @return DataFlow object that may have been transformed
    */
  def mapOption[R >: this.type](f: this.type => Option[R]): R = f(this).getOrElse(this)

  /**
    * Creates new state of the dataflow by adding an input.
    * Duplicate labels are handled in [[prepareForExecution()]]
    *
    * @param label - name of the input
    * @param value - values of the input
    * @return - new state with the input
    */
  def addInput(label: String, value: Option[T]): this.type = {
    if (inputs.labels.contains(label)) throw new DataFlowException(s"Input label [$label] already exists")
    val newInputs = inputs + (label -> value)
    createInstance(newInputs, actions, tagState).asInstanceOf[this.type]
  }

  /**
    * Creates new state of the data flow by replacing the action that is intercepted
    * with action that intercepts it.
    * The action to replace will differ from the intercepted action in the InterceptorAction in the
    * case of replacing an existing InterceptorAction
    *
    * @param interceptor
    * @return
    */
  def addInterceptor(interceptor: InterceptorAction[T, C], guidToIntercept: String): this.type = {
    val newActions = actions.foldLeft(Seq.empty[DataFlowAction[T, C]]) { (res, action) =>
      if (action.guid == guidToIntercept) res :+ interceptor else res :+ action
    }
    val newTagState = {
      tagState.copy(taggedActions = tagState.taggedActions.map {
        kv =>
          if (kv._1 == guidToIntercept) interceptor.guid -> kv._2
          else kv
      })
    }
    createInstance(inputs, newActions, newTagState).asInstanceOf[this.type]
  }

  /**
    * Tag all actions added during the taggedFlow lambda function with any given number of tags.
    * These tags can then be used by the [[tagDependency()]] action to create a dependency in the running order
    * of actions by tag.
    *
    * @param tags       Tags to apply to added actions
    * @param taggedFlow An intermediate flow that actions can be added to that will be be marked with the tag
    * @return
    */
  def tag[S <: DataFlow[T, C]](tags: String*)(taggedFlow: this.type => S): this.type = {
    val (alreadyActiveTags, newTags) = tags.toSet.partition(tagState.activeTags.contains)
    logInfo(s"The following tags are already active, therefore the outer (wider) tagging scope will take precedence: ${alreadyActiveTags.mkString(", ")}")
    val newTagState = tagState.copy(activeTags = tagState.activeTags union newTags)
    val intermediateFlow = taggedFlow(createInstance(inputs, actions, newTagState).asInstanceOf[this.type])
    val finalTagState = intermediateFlow.tagState.copy(activeTags = intermediateFlow.tagState.activeTags diff newTags)
    createInstance(intermediateFlow.inputs, intermediateFlow.actions, finalTagState).asInstanceOf[this.type]
  }

  /**
    * Mark all actions added during the tagDependentFlow lambda function as having a dependency on the tags provided.
    * These actions will only be run once all tagged actions have finished.
    *
    * @param depTags          Tags to create a dependency on
    * @param tagDependentFlow An intermediate flow that actions can be added to that will depended on tagged actions to have completed before running
    * @return
    */
  def tagDependency[S <: DataFlow[T, C]](depTags: String*)(tagDependentFlow: this.type => S): this.type = {
    val (alreadyActiveDeps, newDeps) = depTags.toSet.partition(tagState.activeDependentOnTags.contains)
    logInfo(s"The following tag dependencies are already active, therefore the outer (wider) tag dependency scope will take precedence: ${alreadyActiveDeps.mkString(", ")}")
    val newTagState = tagState.copy(activeDependentOnTags = tagState.activeDependentOnTags union newDeps)
    val intermediateFlow = tagDependentFlow(createInstance(inputs, actions, newTagState).asInstanceOf[this.type])
    val finalTagState = intermediateFlow.tagState.copy(activeDependentOnTags = intermediateFlow.tagState.activeDependentOnTags diff newDeps)
    createInstance(intermediateFlow.inputs, intermediateFlow.actions, finalTagState).asInstanceOf[this.type]
  }

  /**
    * Output labels are unique. Finds action that produces outputLabel.
    *
    * @param outputLabel
    * @return
    */
  def getActionByOutputLabel(outputLabel: String): DataFlowAction[T, C] = {
    actions.find(_.outputLabels.contains(outputLabel)).getOrElse(throw new DataFlowException(s"There is no output label [${outputLabel}] in the flow."))
  }

  /**
    * Guids are unique, find action by guid
    *
    * @param actionGuid
    * @return
    */
  def getActionByGuid(actionGuid: String): DataFlowAction[T, C] = {
    actions.find(actionGuid == _.guid).getOrElse(throw new DataFlowException(s"There is no action with guid [${actionGuid}] in the flow."))
  }

  /**
    * Creates new state of the dataflow by removing executed action from the actions list and adds its outputs to the inputs.
    *
    * @param executed - executed actions
    * @param outputs  - outputs of the executed action
    * @return - next stage data flow without the executed action, but with its outpus as inputs
    * @throws DataFlowException if number of provided outputs is not equal to the number of output labels of the action
    */
  def executed(executed: DataFlowAction[T, C], outputs: Seq[Option[T]]): DataFlow[T, C] = {
    if (outputs.size != executed.outputLabels.size) throw new DataFlowException(s"Action produced different number of results. Expected ${executed.outputLabels.size}, but was ${outputs.size}. ${executed.logLabel}")
    val newActions = actions.filter(_.guid != executed.guid)
    val newInputs = executed.outputLabels.zip(outputs).foldLeft(inputs)((resInput, value) => resInput + value)
    createInstance(newInputs, newActions, tagState)
  }

  /**
    * Returns actions that are ready to run:
    * 1. have no input labels;
    * 2. whose inputs have been created
    * 3. all actions whose dependent tags have been run
    *
    * will not include actions that are skipped.
    *
    * @return
    */
  def nextRunnable(): Seq[DataFlowAction[T, C]] = {
    val withInputs = actions.filter { ac =>
      ac.flowState(inputs) match {
        case ReadyToRun(_) if actionHasNoTagDependencies(ac) => true
        case _ => false
      }
    }
    withInputs
  }

  private def actionHasNoTagDependencies(action: DataFlowAction[T, C]): Boolean = {
    // All tags that this action depends on
    val actionTagDeps = tagState.taggedActions.get(action.guid).map(_.dependentOnTags).getOrElse(Set.empty)
    // Filter actions to produce a list that contains only actions that are tagged with the above tags
    val dependentActions = actions.map(a => tagState.taggedActions.get(a.guid).map(_.tags).getOrElse(Set.empty)).map(_ intersect actionTagDeps).filter(_.nonEmpty)
    // List will be empty if there are no dependent tags left to run
    dependentActions.isEmpty
  }

  private[dataflow] def labelIsInputOrProduced(label: String): Boolean = inputs.entities.contains(label) || actions.exists(a => a.outputLabels.contains(label))

  private def findDuplicateOutputLabels: Set[String] = {
    val allLabels = actions.flatMap(_.outputLabels) ++ inputs.labels
    allLabels.diff(allLabels.distinct).toSet
  }

  /**
    * All new states of the dataflow must be created via this factory method.
    * This will allow specific dataflows to pass their specific context objects into new state.
    *
    * @param in - input entities for the next state
    * @param ac - actions for the next state
    * @return - new instance of the implementing class
    */
  protected def createInstance(in: DataFlowEntities[Option[T]], ac: Seq[DataFlowAction[T, C]], tags: DataFlowTagState): DataFlow[T, C]

  /**
    * A function called just before the flow is executed.
    * By default, this function has just checks the tagging state of the flow, and could be overloaded to have implementation specific
    * preparation steps. An overloaded function should call this function first.
    * It would be responsible for preparing an execution environment such as cleaning temporary directories.
    *
    */
  def prepareForExecution(): this.type = {
    isValidFlowDAG match {
      case Success(_) =>
      case Failure(e) => throw e
    }
    this
  }

  /**
    * Flow DAG is valid iff:
    * 1. All output labels and existing input labels unique
    * 2. Each action depends on labels that are produced by actions or already present in inputs
    * 3. Active tags is empty
    * 4. Active dependencies is zero
    * 5. No cyclic dependencies in labels
    * 6. No cyclic dependencies in tags
    * 7. No cyclic dependencies in label tag combination
    *
    * @return
    */
  def isValidFlowDAG: Try[Unit] = {

    // Condition 1
    val duplicateLabels = findDuplicateOutputLabels
    // Condition 2
    val checkInputDependencies = Try(actions.foreach(a => a.inputLabels.foreach(l => if (!labelIsInputOrProduced(l)) throw new DataFlowException(s"Input label [$l] is not produced by any previous actions"))))

    if (duplicateLabels.nonEmpty) Failure(new DataFlowException(s"Duplicate output labels found: The following labels were found as outputs to multiple actions and/or were in existing flow inputs: ${duplicateLabels.mkString(", ")}"))
    else if (checkInputDependencies.isFailure) checkInputDependencies

    //Condition 3
    else if (tagState.activeTags.nonEmpty) Failure(new DataFlowException(s"Attempted to execute a flow whilst inside the following tag blocks: [${tagState.activeTags.mkString(", ")}]"))

    // Condition 4
    else if (tagState.activeDependentOnTags.nonEmpty) Failure(new DataFlowException(s"Attempted to execute a flow whilst inside the following tag dependency blocks: [${tagState.activeDependentOnTags.mkString(", ")}]"))

    // Conditions 5, 6 and 7
    else isValidDependencyState
  }

  /**
    * Check the tag hierarchy is a correct DAG (PolyTree).
    * This uses a bruteforce approach by getting all actions with any dependencies (tags or inputs)
    * and working up the dependency tree of each one.
    *
    * This works by doing the following, starting with every action in tree:
    * - Take head of actionsToResolve:
    * - To check cyclic references in labels check current action inputs does not contain any labels that have already
    * been seen as outputs, add outputs from current action to seen outputs, add to list of actionsToResolve every action whose
    * outputs are inputs to the current action.
    * - To check cyclic references in tags, for the current action, if it does not depend on any tags skip. If it does
    * check there is at least one action for that tag. Then get all actions for that given tag, three will be a circular dependency
    * if that action guid has been seen before. If no error, add to list of actionsToResolve all actions that this action depends on.
    * - Combination of cyclic references in tags and labels will be covered by the above checks if we always check for duplicate seen outputs and
    * seen actions and a cyclic reference will always be triggered by the above cases in the case of the combination of cycles.
    *
    * TODO: Optimise the implementation, it recurses through all actions multiple times
    */
  private def isValidDependencyState: Try[Unit] = {

    // Map of tag -> action guid
    val actionsByTag = tagState.taggedActions.toSeq.flatMap(kv => kv._2.tags.map(_ -> kv._1)).groupBy(_._1).mapValues(_.map(_._2).map(getActionByGuid))
    // Map of output label -> action guid
    val actionsByOutputLabel = actions.flatMap(a => a.outputLabels.map(_ -> a)).toMap
    // Get all actions with dependencies
    val actionsWithDependencies = tagState.taggedActions.collect { case kv if kv._2.dependentOnTags.nonEmpty => kv }

    case class LoopObject(action: DataFlowAction[T, C], seenActions: Set[String], seenOutputs: Set[String])

    // Resolve dependent actions independently
    def loop(actionsToResolve: List[LoopObject]): Unit = actionsToResolve match {
      case Nil => Unit
      case h :: tail =>
        // Check current action doesn't contain input that have been seen before
        val cyclicLabels = h.action.inputLabels.toSet intersect h.seenOutputs
        if (cyclicLabels.nonEmpty) throw new DataFlowException(s"Circular reference for input label(s) [${cyclicLabels.mkString(", ")}] when resolving " +
          s"action [${h.action.guid}]. Action uses input labels that itself, a sub-action or tag-dependent sub-action outputs.")

        // Get new actions to check from output labels they emit (skipping labels that exist in the flow input, these are already checked)
        val newActionsFromInputs = (h.action.inputLabels.toSet diff inputs.labels).map(actionsByOutputLabel)

        // Get all tags that this action depends on
        val depTags = actionsWithDependencies.get(h.action.guid).map(_.dependentOnTags).getOrElse(Set.empty)
        val newActionsFromTags = depTags.flatMap {
          t => actionsByTag.getOrElse(t, throw new DataFlowException(s"Could not find any actions tagged with label [$t] when resolving dependent actions for action [${h.action.guid}]"))
        }

        // Check the new actions to add have never been seen before
        val allNewActionGuids = (newActionsFromInputs union newActionsFromTags).map(_.guid)
        val cyclicActions = allNewActionGuids intersect h.seenActions
        if (cyclicActions.nonEmpty) throw new DataFlowException(s"Circular reference for action [${h.action.guid}] as a result of cyclic tag dependency. " +
          s"Action has the following tag dependencies [${depTags.mkString(", ")}] and depends on the following input labels [${h.action.inputLabels.toSet.mkString(", ")}]")

        // If we got to this point we're okay, so add new actions and recurse
        val newSeenActions = h.seenActions + h.action.guid
        val newSeenOutputs = h.seenOutputs union h.action.outputLabels.toSet
        val newActionsToResolve = allNewActionGuids.map(getActionByGuid).map(a => LoopObject(a, newSeenActions, newSeenOutputs)).toList
        loop(newActionsToResolve ++ tail)
    }

    Try(loop(actions.map(LoopObject(_, Set.empty[String], Set.empty[String])).toList))

  }

}

/**
  * Represents the tag state on a given action
  *
  * @param tags            Tags belonging to this action
  * @param dependentOnTags Tags this action is dependent on
  */
case class DataFlowActionTags(tags: Set[String], dependentOnTags: Set[String])

/**
  * Represents the tag state on a DataFlow
  *
  * @param activeTags            Tags currently active on the flow (i.e. within the `tag()` context)
  * @param activeDependentOnTags Tag dependencies currently active on the flow (i.e. within the `tagDependency()` context)
  * @param taggedActions         Mapping of actions to their applied tag state
  */
case class DataFlowTagState(activeTags: Set[String], activeDependentOnTags: Set[String], taggedActions: Map[String, DataFlowActionTags])