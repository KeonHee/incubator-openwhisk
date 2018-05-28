/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.loadBalancer

import java.nio.charset.StandardCharsets
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.LongAdder

import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.event.Logging.InfoLevel
import akka.http.scaladsl.model.StatusCodes.TooManyRequests
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import whisk.common.LoggingMarkers._
import whisk.common._
import whisk.connector.kafka.KafkaMessagingProvider
import whisk.core.WhiskConfig._
import whisk.core.connector._
import whisk.core.controller.RejectRequest
import whisk.core.entitlement.Privilege.ACTIVATE
import whisk.core.entitlement._
import whisk.core.entity._
import whisk.core.{ConfigKeys, WhiskConfig}
import whisk.http.Messages.systemOverloaded
import whisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Set
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * A loadbalancer that uses "horizontal" sharding to not collide with fellow loadbalancers.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available, those will be divided to 8 slots for each loadbalancer (if there are 2).
 */
class ShardingContainerPoolBalancer(config: WhiskConfig, controllerInstance: InstanceId)(
  implicit val actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends LoadBalancer {

  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  /** State related to invocations and throttling */
  private val activations = TrieMap[ActivationId, ActivationEntry]()
  private val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  private val totalActivations = new LongAdder()

  /** State needed for scheduling. */
  private val schedulingState = ShardingContainerPoolBalancerState()()

  actorSystem.scheduler.schedule(0.seconds, 10.seconds) {
    MetricEmitter.emitHistogramMetric(LOADBALANCER_ACTIVATIONS_INFLIGHT(controllerInstance), totalActivations.longValue)
  }

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee, that `updateState` and `updateCluster` are called
   * mutually exclusive and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val (invokersToUse, stepSizes) =
      if (!action.exec.pull) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
    val chosen = if (invokersToUse.nonEmpty) {
      val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace, action.fullyQualifiedName(false))
      val homeInvoker = hash % invokersToUse.size
      val stepSize = stepSizes(hash % stepSizes.size)
      ShardingContainerPoolBalancer.schedule(invokersToUse, schedulingState.invokerSlots, homeInvoker, stepSize)
    } else {
      None
    }

    chosen
      .map { invoker =>
        val entry = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map { _ =>
          entry.promise.future
        }
      }
      .getOrElse(Future.failed(LoadBalancerException("No invokers available")))
  }

  /** 2. Update local state with the to be executed activation */
  private def setupActivation(msg: ActivationMessage,
                              action: ExecutableWhiskActionMetaData,
                              instance: InstanceId): ActivationEntry = {

    totalActivations.increment()
    activationsPerNamespace.getOrElseUpdate(msg.user.uuid, new LongAdder()).increment()

    val timeout = action.limits.timeout.duration.max(TimeLimit.STD_DURATION) + 1.minute
    // Install a timeout handler for the catastrophic case where an active ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the active ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    activations.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(timeout) {
          processCompletion(Left(msg.activationId), msg.transid, forced = true, invoker = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.uuid,
          instance,
          timeoutHandler,
          Promise[Either[ActivationId, WhiskActivation]]())
      })
  }

  private val messagingProvider = SpiLoader.get[MessagingProvider]
  private val messageProducer = messagingProvider.getProducer(config)

  /** 3. Send the activation to the invoker */
  private def sendActivationToInvoker(producer: MessageProducer,
                                      msg: ActivationMessage,
                                      invoker: InstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"invoker${invoker.toInt}"

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = InfoLevel)
      case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  /**
   * Subscribes to active acks (completion messages from the invokers), and
   * registers a handler for received active acks from invokers.
   */
  private val activeAckTopic = s"completed${controllerInstance.toInt}"
  private val maxActiveAcksPerPoll = 128
  private val activeAckPollDuration = 1.second
  private val activeAckConsumer =
    messagingProvider.getConsumer(config, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed(
      "activeack",
      logging,
      activeAckConsumer,
      maxActiveAcksPerPoll,
      activeAckPollDuration,
      processActiveAck)
  })

  /** 4. Get the active-ack message and parse it */
  private def processActiveAck(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    CompletionMessage.parse(raw) match {
      case Success(m: CompletionMessage) =>
        processCompletion(m.response, m.transid, forced = false, invoker = m.invoker)
        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw with $t")
    }
  }

  /** 5. Process the active-ack and update the state accordingly */
  private def processCompletion(response: Either[ActivationId, WhiskActivation],
                                tid: TransactionId,
                                forced: Boolean,
                                invoker: InstanceId): Unit = {
    val aid = response.fold(l => l, r => r.activationId)

    // treat left as success (as it is the result of a message exceeding the bus limit)
    val isSuccess = response.fold(_ => true, r => !r.response.isWhiskError)

    activations.remove(aid) match {
      case Some(entry) =>
        totalActivations.decrement()
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())
        schedulingState.invokerSlots.lift(invoker.toInt).foreach(_.release())

        if (!forced) {
          entry.timeoutHandler.cancel()
          entry.promise.trySuccess(response)
        } else {
          entry.promise.tryFailure(new Throwable("no active ack received"))
        }

        logging.info(this, s"${if (!forced) "received" else "forced"} active ack for '$aid'")(tid)
        // Active acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        invokerPool ! InvocationFinishedMessage(invoker, isSuccess)
      case None if !forced =>
        // the entry has already been removed but we receive an active ack for this activation Id.
        // This happens for health actions, because they don't have an entry in Loadbalancerdata or
        // for activations that already timed out.
        invokerPool ! InvocationFinishedMessage(invoker, isSuccess)
        logging.debug(this, s"received active ack for '$aid' which has no entry")(tid)
      case None =>
        // the entry has already been removed by an active ack. This part of the code is reached by the timeout.
        // As the active ack is already processed we don't have to do anything here.
        logging.debug(this, s"forced active ack for '$aid' which has no entry")(tid)
    }
  }

  private val invokerPool = {
    InvokerPool.prepare(controllerInstance, WhiskEntityStore.datastore())

    actorSystem.actorOf(
      InvokerPool.props(
        (f, i) => f.actorOf(InvokerActor.props(i, controllerInstance)),
        (m, i) => sendActivationToInvoker(messageProducer, m, i),
        messagingProvider.getConsumer(config, s"health${controllerInstance.toInt}", "health", maxPeek = 128),
        Some(monitor)))
  }

  /**
    *  The helper methods deliver loadbalancer data to throttler
    */
  def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue()).getOrElse(0))
  def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue())
  def clusterSize: Int = schedulingState.clusterSize

}

class ShardingContainerThrottler(config: WhiskConfig,
                                 controllerInstance: InstanceId,
                                 loadBalancer: LoadBalancer)(
  implicit actorSystem: ActorSystem,
  logging: Logging)
  extends Throttler {

  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  require(loadBalancer.isInstanceOf[ShardingContainerPoolBalancer])
  private val shardingContainerPoolBalancer = loadBalancer.asInstanceOf[ShardingContainerPoolBalancer]

  /**
    * Allows 20% of additional requests on top of the limit to mitigate possible unfair round-robin loadbalancing between
    * controllers
    */
  private def overcommit(clusterSize: Int): Double = if (clusterSize > 1) 1.2 else 1
  private def dilateLimit(limit: Int): Int = Math.ceil(limit.toDouble * overcommit(shardingContainerPoolBalancer.clusterSize)).toInt

  /**
    * Calculates a possibly dilated limit relative to the current user.
    *
    * @param defaultLimit the default limit across the whole system
    * @param user the user to apply that limit to
    * @return a calculated limit
    */
  private def calculateLimit(defaultLimit: Int, overrideLimit: Identity => Option[Int])(user: Identity): Int = {
    val absoluteLimit = overrideLimit(user).getOrElse(defaultLimit)
    dilateLimit(absoluteLimit)
  }

  /**
    * Calculates a limit which applies only to this instance individually.
    *
    * The state needed to correctly check this limit is not shared between all instances, which want to check that
    * limit, so it needs to be divided between the parties who want to perform that check.
    *
    * @param defaultLimit the default limit across the whole system
    * @param user the user to apply that limit to
    * @return a calculated limit
    */
  private def calculateIndividualLimit(defaultLimit: Int, overrideLimit: Identity => Option[Int])(
    user: Identity): Int = {
    val limit = calculateLimit(defaultLimit, overrideLimit)(user)
    if (limit == 0) {
      0
    } else {
      // Edge case: Iff the divided limit is < 1 no loadbalancer would allow an action to be executed, thus we range
      // bound to at least 1
      (limit / shardingContainerPoolBalancer.clusterSize).max(1)
    }
  }

  private val invokeRateThrottler =
    new RateThrottler(
      "actions per minute",
      calculateIndividualLimit(config.actionInvokePerMinuteLimit.toInt, _.limits.invocationsPerMinute))
  private val triggerRateThrottler =
    new RateThrottler(
      "triggers per minute",
      calculateIndividualLimit(config.triggerFirePerMinuteLimit.toInt, _.limits.firesPerMinute))

  private val concurrentInvokeThrottler =
    new ActivationThrottler(
      calculateIndividualLimit(config.actionInvokeConcurrentLimit.toInt, _.limits.concurrentInvocations),
      shardingContainerPoolBalancer.activeActivationsFor,
      shardingContainerPoolBalancer.totalActiveActivations,
      config.actionInvokeSystemOverloadLimit.toInt)

  private val eventProducer = KafkaMessagingProvider.getProducer(config)

  /**
    * Limits activations if the system is overloaded.
    *
    * @param right the privilege, if ACTIVATE then check quota else return None
    * @return future completing successfully if system is not overloaded else failing with a rejection
    */
  protected def checkSystemOverload(right: Privilege)(implicit transid: TransactionId): Future[Unit] = {
    concurrentInvokeThrottler.isOverloaded.flatMap { isOverloaded =>
      val systemOverload = right == ACTIVATE && isOverloaded
      if (systemOverload) {
        logging.error(this, "system is overloaded")
        Future.failed(RejectRequest(TooManyRequests, systemOverloaded))
      } else Future.successful(())
    }
  }

  /**
    * Limits activations if subject exceeds their own limits.
    * If the requested right is an activation, the set of resources must contain an activation of an action or filter to be throttled.
    * While it is possible for the set of resources to contain more than one action or trigger, the plurality is ignored and treated
    * as one activation since these should originate from a single macro resources (e.g., a sequence).
    *
    * @param user the subject identity to check rights for
    * @param right the privilege, if ACTIVATE then check quota else return None
    * @param resources the set of resources must contain at least one resource that can be activated else return None
    * @return future completing successfully if user is below limits else failing with a rejection
    */
  private def checkUserThrottle(user: Identity, right: Privilege, resources: Set[Resource])(
    implicit transid: TransactionId): Future[Unit] = {
    if (right == ACTIVATE) {
      if (resources.exists(_.collection.path == Collection.ACTIONS)) {
        checkThrottleOverload(Future.successful(invokeRateThrottler.check(user)), user)
      } else if (resources.exists(_.collection.path == Collection.TRIGGERS)) {
        checkThrottleOverload(Future.successful(triggerRateThrottler.check(user)), user)
      } else Future.successful(())
    } else Future.successful(())
  }

  /**
    * Limits activations if subject exceeds limit of concurrent invocations.
    * If the requested right is an activation, the set of resources must contain an activation of an action to be throttled.
    * While it is possible for the set of resources to contain more than one action, the plurality is ignored and treated
    * as one activation since these should originate from a single macro resources (e.g., a sequence).
    *
    * @param user the subject identity to check rights for
    * @param right the privilege, if ACTIVATE then check quota else return None
    * @param resources the set of resources must contain at least one resource that can be activated else return None
    * @return future completing successfully if user is below limits else failing with a rejection
    */
  private def checkConcurrentUserThrottle(user: Identity, right: Privilege, resources: Set[Resource])(
    implicit transid: TransactionId): Future[Unit] = {
    if (right == ACTIVATE && resources.exists(_.collection.path == Collection.ACTIONS)) {
      checkThrottleOverload(concurrentInvokeThrottler.check(user), user)
    } else Future.successful(())
  }

  private def checkThrottleOverload(throttle: Future[RateLimit], user: Identity)(
    implicit transid: TransactionId): Future[Unit] = {
    throttle.flatMap { limit =>
      val userId = user.authkey.uuid
      if (limit.ok) {
        limit match {
          case c: ConcurrentRateLimit => {
            val metric =
              Metric("ConcurrentInvocations", c.count + 1)
            UserEvents.send(
              eventProducer,
              EventMessage(
                s"controller${controllerInstance.instance}",
                metric,
                user.subject,
                user.namespace.toString,
                userId,
                metric.typeName))
          }
          case _ => // ignore
        }
        Future.successful(())
      } else {
        logging.info(this, s"'${user.namespace}' has exceeded its throttle limit, ${limit.errorMsg}")
        val metric = Metric(limit.limitName, 1)
        UserEvents.send(
          eventProducer,
          EventMessage(
            s"controller${controllerInstance.instance}",
            metric,
            user.subject,
            user.namespace.toString,
            userId,
            metric.typeName))
        Future.failed(RejectRequest(TooManyRequests, limit.errorMsg))
      }
    }
  }

  override def check(user: Identity, right: Privilege, resources: Set[Resource])(
    implicit transid: TransactionId): Future[Unit] = {
    if(resources.isEmpty) {
      checkSystemOverload(right)
        .flatMap(_ => checkUserThrottle(user, right, resources))
        .flatMap(_ => checkConcurrentUserThrottle(user, right, resources))
    } else {
      checkSystemOverload(ACTIVATE)
        .flatMap(_ => checkThrottleOverload(Future.successful(invokeRateThrottler.check(user)), user))
        .flatMap(_ => checkThrottleOverload(concurrentInvokeThrottler.check(user), user))
    }
  }
}

object ShardingContainerPoolBalancer extends LoadBalancerProvider {

  override def loadBalancer(whiskConfig: WhiskConfig, instance: InstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = new ShardingContainerPoolBalancer(whiskConfig, instance)

  override def throttler(whiskConfig: WhiskConfig, instance: InstanceId, loadBalancer: LoadBalancer)(
    implicit actorSystem: ActorSystem,
    logging: Logging): Throttler = new ShardingContainerThrottler(whiskConfig, instance, loadBalancer)

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def schedule(invokers: IndexedSeq[InvokerHealth],
               dispatched: IndexedSeq[ForcableSemaphore],
               index: Int,
               step: Int,
               stepsDone: Int = 0)(implicit logging: Logging): Option[InstanceId] = {
    val numInvokers = invokers.size

    if (numInvokers > 0) {
      val invoker = invokers(index)
      // If the current invoker is healthy and we can get a slot
      if (invoker.status == Healthy && dispatched(invoker.id.toInt).tryAcquire()) {
        Some(invoker.id)
      } else {
        // If we've gone through all invokers
        if (stepsDone == numInvokers + 1) {
          val healthyInvokers = invokers.filter(_.status == Healthy)
          if (healthyInvokers.nonEmpty) {
            // Choose a healthy invoker randomly
            val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
            dispatched(random.toInt).forceAcquire()
            logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
            Some(random)
          } else {
            None
          }
        } else {
          val newIndex = (index + step) % numInvokers
          schedule(invokers, dispatched, newIndex, step, stepsDone + 1)
        }
      }
    } else {
      None
    }
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _managedStepSizes the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots state of accessible slots of each invoker
 */
case class ShardingContainerPoolBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _invokerSlots: IndexedSeq[ForcableSemaphore] = IndexedSeq.empty[ForcableSemaphore],
  private var _clusterSize: Int = 1)(
  lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  private val totalInvokerThreshold = lbConfig.invokerBusyThreshold
  private var currentInvokerThreshold = totalInvokerThreshold

  private val blackboxFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"blackboxFraction = $blackboxFraction")(TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def managedStepSizes: Seq[Int] = _managedStepSizes
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def invokerSlots: IndexedSeq[ForcableSemaphore] = _invokerSlots
  def clusterSize: Int = _clusterSize

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to `updateCluster`
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    val blackboxes = Math.max(1, (newSize.toDouble * blackboxFraction).toInt)
    val managed = Math.max(1, newSize - blackboxes)

    _invokers = newInvokers
    _blackboxInvokers = _invokers.takeRight(blackboxes)
    _managedInvokers = _invokers.take(managed)

    if (oldSize != newSize) {
      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)

      if (oldSize < newSize) {
        // Keeps the existing state..
        _invokerSlots = _invokerSlots ++ IndexedSeq.fill(newSize - oldSize) {
          new ForcableSemaphore(currentInvokerThreshold)
        }
      }
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to `updateState`
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      _clusterSize = actualSize
      val newTreshold = (totalInvokerThreshold / actualSize) max 1 // letting this fall below 1 doesn't make sense
      currentInvokerThreshold = newTreshold
      _invokerSlots = _invokerSlots.map(_ => new ForcableSemaphore(currentInvokerThreshold))

      logging.info(
        this,
        s"loadbalancer cluster size changed to $actualSize active nodes. invokerThreshold = $currentInvokerThreshold")(
        TransactionId.loadbalancer)
    }
  }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param invokerBusyThreshold how many slots an invoker has available in total
 */
case class ShardingContainerPoolBalancerConfig(blackboxFraction: Double, invokerBusyThreshold: Int)

/**
 * State kept for each activation until completion.
 *
 * @param id id of the activation
 * @param namespaceId namespace that invoked the action
 * @param invokerName invoker the action is scheduled to
 * @param timeoutHandler times out completion of this activation, should be canceled on good paths
 * @param promise the promise to be completed by the activation
 */
case class ActivationEntry(id: ActivationId,
                           namespaceId: UUID,
                           invokerName: InstanceId,
                           timeoutHandler: Cancellable,
                           promise: Promise[Either[ActivationId, WhiskActivation]])
