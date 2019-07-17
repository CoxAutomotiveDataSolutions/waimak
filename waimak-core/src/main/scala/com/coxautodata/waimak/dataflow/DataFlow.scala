package com.coxautodata.waimak.dataflow

import com.coxautodata.waimak.log.Logging

import scala.util.{Success, Try}

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
  */
trait DataFlow[Self <: DataFlow[Self]] extends Logging {
  this: Self =>

  def flowContext: FlowContext

  def schedulingMeta: SchedulingMeta

  def schedulingMeta(sc: SchedulingMeta): Self

  def setExtensionMetadata(newMetadata: Map[DataFlowExtension[Self], DataFlowMetadataState]): Self

  def extensionMetadata: Map[DataFlowExtension[Self], DataFlowMetadataState]

  def updateExtensionMetadata(extensionKey: DataFlowExtension[Self], updateMeta: DataFlowMetadataState => DataFlowMetadataState): Self = {

    val oldMeta = extensionMetadata.getOrElse(extensionKey, extensionKey.initialState)
    setExtensionMetadata(extensionMetadata.updated(extensionKey, updateMeta(oldMeta)))

  }

  /**
    * Current [[DataFlowExecutor]] associated with this flow
    *
    * @return
    */
  def executor: DataFlowExecutor

  /**
    * Add a new executor to this flow, replacing the existing one
    *
    * @param executor [[DataFlowExecutor]] to add to this flow
    */
  def withExecutor(executor: DataFlowExecutor): Self

  /**
    * Execute this flow using the current [[executor]] on the flow.
    * See [[DataFlowExecutor.execute()]] for more information.
    */
  def execute(errorOnUnexecutedActions: Boolean = true): (Seq[DataFlowAction], Self) = executor.execute(this, errorOnUnexecutedActions)

  /**
    * Inputs that were explicitly set or produced by previous actions, these are inputs for all following actions.
    * Inputs are preserved in the data flow state, even if they are no longer required by the remaining actions.
    * //TODO: explore the option of removing the inputs that are no longer required by remaining actions!!!
    *
    * @return
    */
  def inputs: DataFlowEntities

  def inputs(inp: DataFlowEntities): Self

  /**
    * Actions to execute, these will be scheduled when inputs become available. Executed actions must be removed from
    * the sate.
    *
    * @return
    */
  def actions: Seq[DataFlowAction]

  def actions(acs: Seq[DataFlowAction]): Self

  def tagState: DataFlowTagState

  def tagState(ts: DataFlowTagState): Self

  /**
    * Creates new state of the dataflow by adding an action to it.
    *
    * @param action - action to add
    * @return - new state with action
    * @throws  DataFlowException when:
    *                            1) at least one of the input labels is not present in the inputs
    *                            2) at least one of the input labels is not present in the outputs of existing actions
    */
  def addAction[A <: DataFlowAction](action: A): Self = {

    action.outputLabels.foreach(l => if (labelIsInputOrProduced(l)) {
      throw new DataFlowException(s"Output label [${l}] is already in the inputs or is produced by another action.")
    } else {
      Unit
    })
    val newActions = actions :+ action
    // Add current action into tagstate with current active tags/dep tags
    val newTagState = tagState.copy(taggedActions = tagState.taggedActions + (action.guid -> DataFlowActionTags(tagState.activeTags, tagState.activeDependentOnTags)))
    actions(newActions).tagState(newTagState).schedulingMeta(schedulingMeta.addAction(action))
  }

  /**
    * Transforms the current dataflow by applying a function to it.
    *
    * @param f A function that transforms a dataflow object
    * @return New dataflow
    */
  def map[R >: Self](f: Self => R): R = f(this)

  /**
    * Optionally transform a dataflow depending on the output of the
    * applying function. If the transforming function returns a None
    * then the original dataflow is returned.
    *
    * @param f A function that returns an Option[DataFlow]
    * @return DataFlow object that may have been transformed
    */
  def mapOption[R >: Self](f: Self => Option[R]): R = f(this).getOrElse(this)

  /**
    * Fold left over a collection, where the current DataFlow is the zero value.
    * Lets you fold over a flow inline in the flow.
    *
    * @param foldOver Collection to fold over
    * @param f        Function to apply during the flow
    * @return A DataFlow produced after repeated applications of f for each element in the collection
    */
  def foldLeftOver[A, S >: Self](foldOver: Iterable[A])(f: (S, A) => S): S = {
    foldOver.foldLeft[S](this)(f)
  }

  /**
    * Creates new state of the dataflow by adding an input.
    * Duplicate labels are handled in [[prepareForExecution()]]
    *
    * @param label - name of the input
    * @param value - values of the input
    * @return - new state with the input
    */
  def addInput(label: String, value: Option[Any]): Self = {
    if (inputs.labels.contains(label)) throw new DataFlowException(s"Input label [$label] already exists")
    inputs(inputs + (label -> value))
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
  def addInterceptor(interceptor: InterceptorAction, guidToIntercept: String): Self = {
    val newActions = actions.foldLeft(Seq.empty[DataFlowAction]) { (res, action) =>
      if (action.guid == guidToIntercept) res :+ interceptor else res :+ action
    }
    val newTagState = {
      val newInterceptorTags = tagState.taggedActions
        .get(guidToIntercept)
        .map(actionTags => actionTags.copy(tags = actionTags.tags union tagState.activeTags, dependentOnTags = actionTags.dependentOnTags union tagState.activeDependentOnTags))
        .getOrElse(DataFlowActionTags(tagState.activeTags, tagState.activeDependentOnTags))

      tagState.copy(taggedActions = tagState.taggedActions - guidToIntercept + (interceptor.guid -> newInterceptorTags))
    }
    //interceptors are not added to the execution pools
    actions(newActions).tagState(newTagState)
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
  def tag(tags: String*)(taggedFlow: Self => Self): Self = {
    val (alreadyActiveTags, newTags) = tags.toSet.partition(tagState.activeTags.contains)

    alreadyActiveTags
      .reduceLeftOption((z, t) => s"$z, $t")
      .foreach(t => logInfo(s"The following tags are already active, therefore the outer (wider) tagging scope will take precedence: $t"))

    val newTagState = tagState.copy(activeTags = tagState.activeTags union newTags)
    val intermediateFlow = taggedFlow(tagState(newTagState))
    val finalTagState = intermediateFlow.tagState.copy(activeTags = intermediateFlow.tagState.activeTags diff newTags)
    intermediateFlow.tagState(finalTagState)
  }

  /**
    * Mark all actions added during the tagDependentFlow lambda function as having a dependency on the tags provided.
    * These actions will only be run once all tagged actions have finished.
    *
    * @param depTags          Tags to create a dependency on
    * @param tagDependentFlow An intermediate flow that actions can be added to that will depended on tagged actions to have completed before running
    * @return
    */
  def tagDependency(depTags: String*)(tagDependentFlow: Self => Self): Self = {
    val (alreadyActiveDeps, newDeps) = depTags.toSet.partition(tagState.activeDependentOnTags.contains)

    alreadyActiveDeps
      .reduceLeftOption((z, t) => s"$z, $t")
      .foreach(t => logInfo(s"The following tag dependencies are already active, therefore the outer (wider) tag dependency scope will take precedence: $t"))

    val newTagState = tagState.copy(activeDependentOnTags = tagState.activeDependentOnTags union newDeps)
    val intermediateFlow = tagDependentFlow(tagState(newTagState))
    val finalTagState = intermediateFlow.tagState.copy(activeDependentOnTags = intermediateFlow.tagState.activeDependentOnTags diff newDeps)
    intermediateFlow.tagState(finalTagState)
  }

  /**
    * Creates a code block with all actions inside of it being run on the specified execution pool. Same execution pool
    * name can be used multiple times and nested pools are allowed, the name closest to the action will be assigned to it.
    *
    * Ex:
    * flow.executionPool("pool_1") {
    * _.addAction(a1)
    * .addAction(a2)
    * .executionPool("pool_2") {
    * _.addAction(a3)
    * .addAction(a4)
    * }..addAction(a5)
    * }
    *
    * So actions a1, a2, a5 will be in the pool_1 and actions a3, a4 in the pool_2
    *
    * @param executionPoolName pool name to assign to all actions inside of it, but it can be overwritten by the nested execution pools.
    * @param nestedFlow
    * @return
    */
  def executionPool(executionPoolName: String)(nestedFlow: Self => Self): Self = schedulingMeta(_.setExecutionPoolName(executionPoolName))(nestedFlow)

  /**
    * Generic method that can be used to add context and state to all actions inside the block.
    *
    * @param mutateState function that adds attributes to the state
    * @param nestedFlow  all actions inside of this flow will be associated with the mutated state
    * @return
    */
  def schedulingMeta(mutateState: SchedulingMetaState => SchedulingMetaState)(nestedFlow: Self => Self): Self = {
    val previousState = schedulingMeta.state
    val nestedMeta = schedulingMeta.setState(mutateState(previousState))
    val intermediateFlow = nestedFlow(schedulingMeta(nestedMeta))
    intermediateFlow.schedulingMeta(intermediateFlow.schedulingMeta.setState(previousState))
  }

  /**
    * Output labels are unique. Finds action that produces outputLabel.
    *
    * @param outputLabel
    * @return
    */
  def getActionByOutputLabel(outputLabel: String): DataFlowAction = {
    actions.find(_.outputLabels.contains(outputLabel)).getOrElse(throw new DataFlowException(s"There is no output label [${outputLabel}] in the flow."))
  }

  /**
    * Guids are unique, find action by guid
    *
    * @param actionGuid
    * @return
    */
  def getActionByGuid(actionGuid: String): DataFlowAction = {
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
  def executed(executed: DataFlowAction, outputs: Seq[Option[Any]]): Self = {
    if (outputs.size != executed.outputLabels.size) throw new DataFlowException(s"Action produced different number of results. Expected ${executed.outputLabels.size}, but was ${outputs.size}. ${executed.logLabel}")
    val newActions = actions.filter(_.guid != executed.guid)
    val newInputs = executed.outputLabels.zip(outputs).foldLeft(inputs)((resInput, value) => resInput + value)
    actions(newActions).inputs(newInputs).schedulingMeta(schedulingMeta.removeAction(executed))
  }

  /**
    * Returns actions that are ready to run:
    * 1. have no input labels;
    * 2. whose inputs have been created
    * 3. all actions whose dependent tags have been run
    * 4. belong to the available pool
    *
    * will not include actions that are skipped.
    *
    * @param executionPoolsAvailable set of execution pool for which to schedule actions
    * @return
    */
  def nextRunnable(executionPoolsAvailable: Set[String]): Seq[DataFlowAction] = {
    val withInputs = actions
      .filter(ac => executionPoolsAvailable.contains(schedulingMeta.executionPoolName(ac)))
      .filter { ac =>
        ac.flowState(inputs) match {
          case ReadyToRun(_) if actionHasNoTagDependencies(ac) => true
          case _ => false
        }
      }
    withInputs
  }

  private def actionHasNoTagDependencies(action: DataFlowAction): Boolean = {
    // All tags that this action depends on
    val actionTagDeps = tagState.taggedActions.get(action.guid).map(_.dependentOnTags).getOrElse(Set.empty)
    // Filter actions to produce a list that contains only actions that are tagged with the above tags
    val dependentActions = actions.map(a => tagState.taggedActions.get(a.guid).map(_.tags).getOrElse(Set.empty)).map(_ intersect actionTagDeps).filter(_.nonEmpty)
    // List will be empty if there are no dependent tags left to run
    dependentActions.isEmpty
  }

  private[dataflow] def labelIsInputOrProduced(label: String): Boolean = inputs.contains(label) || actions.exists(a => a.outputLabels.contains(label))

  private def findDuplicateOutputLabels: Set[String] = {
    val allLabels = actions.flatMap(_.outputLabels) ++ inputs.labels
    allLabels.diff(allLabels.distinct).toSet
  }

  /**
    * A function called just before the flow is executed.
    * By default, this function has just checks the tagging state of the flow, and could be overloaded to have implementation specific
    * preparation steps. An overloaded function should call this function first.
    * It would be responsible for preparing an execution environment such as cleaning temporary directories.
    *
    */
  def prepareForExecution(): Try[Self] = {
    //TODO max loops
    def loopUntilStable(flow: Self): Self = {
      val (newFlow, changed) = flow.extensionMetadata
        .foldLeft[(Self, Boolean)]((flow, false)) {
        case ((z, updated), (ex, meta)) =>
          ex.preExecutionManipulation(z, meta) match {
            case None => (z, updated)
            case Some(f) =>
              (f, true)
          }
      }
      if (changed) loopUntilStable(newFlow)
      else newFlow
    }

    Try {
      loopUntilStable(this)
    }
      .flatMap(_.isValidFlowDAG)
  }

  /**
    * A function called just after the flow is executed.
    * By default, the implementation on [[DataFlow]] is no-op,
    * however it is used in [[spark.SparkDataFlow]] to clean up
    * the temporary directory
    *
    */
  def finaliseExecution(): Try[Self] = Success(this)

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
  def isValidFlowDAG: Try[Self] = {

    Try {
      // Condition 1
      val duplicateLabels = findDuplicateOutputLabels
      if (duplicateLabels.nonEmpty) throw new DataFlowException(s"Duplicate output labels found: The following labels were found as outputs to multiple actions and/or were in existing flow inputs: ${duplicateLabels.mkString(", ")}")

      // Condition 2
      actions.foreach { a =>
        a.inputLabels.foreach(l => if (!labelIsInputOrProduced(l)) throw new DataFlowException(s"Input label [$l] is not produced by any previous actions"))
      }

      //Condition 3
      if (tagState.activeTags.nonEmpty) throw new DataFlowException(s"Attempted to execute a flow whilst inside the following tag blocks: [${tagState.activeTags.mkString(", ")}]")

      // Condition 4
      if (tagState.activeDependentOnTags.nonEmpty) throw new DataFlowException(s"Attempted to execute a flow whilst inside the following tag dependency blocks: [${tagState.activeDependentOnTags.mkString(", ")}]")

    }.flatMap { _ =>
      // Conditions 5, 6 and 7
      isValidDependencyState
    }.map(_ => this)
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
    * Optimisation: The actions checked during a chain of actions from an initial action are memoised to prevent the same action chain being checked twice
    */
  private def isValidDependencyState: Try[Unit] = {

    // Map of tag -> action guid
    val actionsByTag = tagState.taggedActions.toSeq.flatMap(kv => kv._2.tags.map(_ -> kv._1)).groupBy(_._1).mapValues(_.map(_._2).map(getActionByGuid))
    // Map of output label -> action guid
    val actionsByOutputLabel = actions.flatMap(a => a.outputLabels.map(_ -> a)).toMap
    // Get all actions with dependencies
    val actionsWithDependencies = tagState.taggedActions.collect { case kv if kv._2.dependentOnTags.nonEmpty => kv }

    case class LoopObject(action: DataFlowAction, seenActions: Set[String], seenOutputs: Set[String])

    // Resolve dependent actions independently
    def loop(actionsToResolve: List[LoopObject], resolvedActions: List[String], provisionallyResolvedActions: List[String]): List[String] = actionsToResolve match {
      case Nil => resolvedActions ++ provisionallyResolvedActions
      case h :: tail if resolvedActions.contains(h.action.guid) => loop(tail, resolvedActions, provisionallyResolvedActions)
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
        loop(newActionsToResolve ++ tail, resolvedActions, h.action.guid +: provisionallyResolvedActions)
    }

    // Fold over all the actions, accumulating fully resolved actions as we go so we don't check the same action multiple times
    Try {
      actions.foldLeft(List.empty[String])((resolvedActions, toResolve) => loop(List(LoopObject(toResolve, Set.empty, Set.empty)), resolvedActions, List.empty))
    }

  }

}

object DataFlow {

  val dataFlowParamPrefix: String = "spark.waimak.dataflow"

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

/** When a Data Flow is defined, certain hints related to its execution can be specified, these hints will help scheduler
  * with deciding when and where to run the action. Further uses can be added to it.
  *
  * At the moment, when an action is added to the scheduling meta, it will automatically assign it the current Execution
  * Pool, but if there were other global context attributes to assign, than the action could aquire them as well.
  *
  * @param state       describes a current state of schedulingMeta
  * @param actionState Map[DataFlowAction.schedulingGuid, Execution Pool Name] - association between actions and execution pool names
  */
case class SchedulingMeta(state: SchedulingMetaState, actionState: Map[String, SchedulingMetaState]) {

  def this() = this(SchedulingMetaState(DEFAULT_POOL_NAME, None), Map.empty)

  /**
    * Adds action to the scheduling meta, action aquires all of the relevant context attributes (like currentExecutionPoolName)
    *
    * @param action action to add to the scheduling meta
    * @return new state of the scheduling meta with action associated with relevant context attributes
    */
  def addAction(action: DataFlowAction): SchedulingMeta = {
    SchedulingMeta(state, actionState + (action.schedulingGuid -> state))
  }

  /**
    * Removes the action from scheduling meta.
    *
    * @param action
    * @return new state of the scheduling meta without the action
    */
  def removeAction(action: DataFlowAction): SchedulingMeta = {
    SchedulingMeta(state, actionState - action.schedulingGuid)
  }

  /**
    * Gets action's execution pool name.
    *
    * @param action
    * @return execution pool name of the action, if not found than returns DEFAULT_POOL_NAME
    */
  def executionPoolName(action: DataFlowAction): String = actionState.get(action.schedulingGuid).map(_.executionPoolName).getOrElse(DEFAULT_POOL_NAME)

  /**
    * Sets current pool name into the context of the scheduling meta.
    *
    * @param newState
    * @return new state of the scheduling meta with new execution pool name, all subsequent actions will be added to it.
    */
  def setState(newState: SchedulingMetaState): SchedulingMeta = SchedulingMeta(newState, actionState)

}

/**
  * Contains values that will be associated with all actions added to the data flow.
  *
  * @param executionPoolName name of the execution pool
  */
case class SchedulingMetaState(executionPoolName: String, context: Option[Any] = None) {

  //TODO: May be add tags into here to have a common place to accumulate extra scheduling info about actions and tags

  def setExecutionPoolName(poolName: String): SchedulingMetaState = SchedulingMetaState(poolName)

  def setContext(cntx: Option[Any]): SchedulingMetaState = SchedulingMetaState(executionPoolName, cntx)

}

trait DataFlowExtension[S <: DataFlow[S]] {

  def initialState: DataFlowMetadataState

  def preExecutionManipulation(flow: S, meta: DataFlowMetadataState): Option[S]

}

trait DataFlowMetadataState {

  def getMetadataAsType[A <: DataFlowMetadataState]: A = {
    Try(this.asInstanceOf[A])
      .recover {
        case e: ClassCastException => throw new DataFlowException("Metadata State object was not of correct type", e)
      }
      .get
  }
}
