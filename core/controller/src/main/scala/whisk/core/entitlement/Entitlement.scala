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

package whisk.core.entitlement

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.Forbidden
import whisk.common.{Logging, TransactionId}
import whisk.core.WhiskConfig
import whisk.core.controller.RejectRequest
import whisk.core.entitlement.Privilege.{ACTIVATE, REJECT}
import whisk.core.entity._
import whisk.core.loadBalancer.Throttler
import whisk.http.{ErrorResponse, Messages}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Set
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

package object types {
  type Entitlements = TrieMap[(Subject, String), Set[Privilege]]
}

/**
 * Resource is a type that encapsulates details relevant to identify a specific resource.
 * It may be an entire collection, or an element in a collection.
 *
 * @param namespace the namespace the resource resides in
 * @param collection the collection (e.g., actions, triggers) identifying a resource
 * @param entity an optional entity name that identifies a specific item in the collection
 * @param env an optional environment to bind to the resource during an activation
 */
protected[core] case class Resource(namespace: EntityPath,
                                    collection: Collection,
                                    entity: Option[String],
                                    env: Option[Parameters] = None) {
  def parent: String = collection.path + EntityPath.PATHSEP + namespace
  def id: String = parent + entity.map(EntityPath.PATHSEP + _).getOrElse("")
  def fqname: String = namespace.asString + entity.map(EntityPath.PATHSEP + _).getOrElse("")
  override def toString: String = id
}

protected[core] object EntitlementProvider {

  val requiredProperties = Map(
    WhiskConfig.actionInvokePerMinuteLimit -> null,
    WhiskConfig.actionInvokeConcurrentLimit -> null,
    WhiskConfig.triggerFirePerMinuteLimit -> null,
    WhiskConfig.actionInvokeSystemOverloadLimit -> null)
}

/**
 * A trait that implements entitlements to resources. It performs checks for CRUD and Acivation requests.
 * This is where enforcement of activation quotas takes place, in additional to basic authorization.
 */
protected[core] abstract class EntitlementProvider(
  config: WhiskConfig,
  throttler: Throttler,
  controllerInstance: InstanceId)(implicit actorSystem: ActorSystem, logging: Logging) {

  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  /**
   * Grants a subject the right to access a resources.
   *
   * @param subject the subject to grant right to
   * @param right the privilege to grant the subject
   * @param resource the resource to grant the subject access to
   * @return a promise that completes with true iff the subject is granted the right to access the requested resource
   */
  protected[core] def grant(subject: Subject, right: Privilege, resource: Resource)(
    implicit transid: TransactionId): Future[Boolean]

  /**
   * Revokes a subject the right to access a resources.
   *
   * @param subject the subject to revoke right to
   * @param right the privilege to revoke the subject
   * @param resource the resource to revoke the subject access to
   * @return a promise that completes with true iff the subject is revoked the right to access the requested resource
   */
  protected[core] def revoke(subject: Subject, right: Privilege, resource: Resource)(
    implicit transid: TransactionId): Future[Boolean]

  /**
   * Checks if a subject is entitled to a resource because it was granted the right explicitly.
   *
   * @param subject the subject to check rights for
   * @param right the privilege the subject is requesting
   * @param resource the resource the subject requests access to
   * @return a promise that completes with true iff the subject is permitted to access the request resource
   */
  protected def entitled(subject: Subject, right: Privilege, resource: Resource)(
    implicit transid: TransactionId): Future[Boolean]

  /**
   * Checks action activation rate throttles for an identity.
   *
   * @param user the identity to check rate throttles for
   * @return a promise that completes with success iff the user is within their activation quota
   */
  protected[core] def checkThrottles(user: Identity)(implicit transid: TransactionId): Future[Unit] = {

    logging.debug(this, s"checking user '${user.subject}' has not exceeded activation quota")
    throttler.check(user, ACTIVATE, Set.empty)
  }

  /**
   * Checks if a subject has the right to access a specific resource. The entitlement may be implicit,
   * that is, inferred based on namespaces that a subject belongs to and the namespace of the
   * resource for example, or explicit. The implicit check is computed here. The explicit check
   * is delegated to the service implementing this interface.
   *
   * NOTE: do not use this method to check a package binding because this method does not allow
   * for a continuation to check that both the binding and the references package are both either
   * implicitly or explicitly granted. Instead, resolve the package binding first and use the alternate
   * method which authorizes a set of resources.
   *
   * @param user the subject to check rights for
   * @param right the privilege the subject is requesting (applies to the entire set of resources)
   * @param resource the resource the subject requests access to
   * @return a promise that completes with success iff the subject is permitted to access the requested resource
   */
  protected[core] def check(user: Identity, right: Privilege, resource: Resource)(
    implicit transid: TransactionId): Future[Unit] = check(user, right, Set(resource))

  /**
   * Constructs a RejectRequest containing the forbidden resources.
   *
   * @param resources resources forbidden to access
   * @return a RejectRequest with the appropriate message
   */
  private def unauthorizedOn(resources: Set[Resource])(implicit transid: TransactionId) = {
    RejectRequest(
      Forbidden,
      Some(
        ErrorResponse(
          Messages.notAuthorizedtoAccessResource(resources.map(_.fqname).toSeq.sorted.toSet.mkString(", ")),
          transid)))
  }

  /**
   * Checks if a subject has the right to access a set of resources. The entitlement may be implicit,
   * that is, inferred based on namespaces that a subject belongs to and the namespace of the
   * resource for example, or explicit. The implicit check is computed here. The explicit check
   * is delegated to the service implementing this interface.
   *
   * @param user the subject identity to check rights for
   * @param right the privilege the subject is requesting (applies to the entire set of resources)
   * @param resources the set of resources the subject requests access to
   * @param noThrottle ignore throttle limits
   * @return a promise that completes with success iff the subject is permitted to access all of the requested resources
   */
  protected[core] def check(user: Identity, right: Privilege, resources: Set[Resource], noThrottle: Boolean = false)(
    implicit transid: TransactionId): Future[Unit] = {
    val subject = user.subject

    val entitlementCheck: Future[Unit] = if (user.rights.contains(right)) {
      if (resources.nonEmpty) {
        logging.debug(this, s"checking user '$subject' has privilege '$right' for '${resources.mkString(", ")}'")
        val throttleCheck =
          if (noThrottle) Future.successful(())
          else
            throttler.check(user, right, resources)
        throttleCheck
          .flatMap(_ => checkPrivilege(user, right, resources))
          .flatMap(checkedResources => {
            val failedResources = checkedResources.filterNot(_._2)
            if (failedResources.isEmpty) Future.successful(())
            else Future.failed(unauthorizedOn(failedResources.map(_._1)))
          })
      } else Future.successful(())
    } else if (right != REJECT) {
      logging.debug(
        this,
        s"supplied authkey for user '$subject' does not have privilege '$right' for '${resources.mkString(", ")}'")
      Future.failed(unauthorizedOn(resources))
    } else {
      Future.failed(unauthorizedOn(resources))
    }

    entitlementCheck andThen {
      case Success(rs) =>
        logging.debug(this, "authorized")
      case Failure(r: RejectRequest) =>
        logging.debug(this, s"not authorized: $r")
      case Failure(t) =>
        logging.error(this, s"failed while checking entitlement: ${t.getMessage}")
    }
  }

  /**
   * NOTE: explicit grants do not work with package bindings because this method does not allow
   * for a continuation to check that both the binding and the references package are both either
   * implicitly or explicitly granted. Instead, the given resource set should include both the binding
   * and the referenced package.
   */
  protected def checkPrivilege(user: Identity, right: Privilege, resources: Set[Resource])(
    implicit transid: TransactionId): Future[Set[(Resource, Boolean)]] = {
    // check the default namespace first, bypassing additional checks if permitted
    val defaultNamespaces = Set(user.namespace.asString)
    implicit val es: EntitlementProvider = this

    Future.sequence {
      resources.map { resource =>
        resource.collection.implicitRights(user, defaultNamespaces, right, resource) flatMap {
          case true => Future.successful(resource -> true)
          case false =>
            logging.debug(this, "checking explicit grants")
            entitled(user.subject, right, resource).flatMap(b => Future.successful(resource -> b))
        }
      }
    }
  }
}

/**
 * A trait to consolidate gathering of referenced entities for various types.
 * Current entities that refer to others: action sequences, rules, and package bindings.
 */
trait ReferencedEntities {

  /**
   * Gathers referenced resources for types knows to refer to others.
   * This is usually done on a PUT request, hence the types are not one of the
   * canonical datastore types. Hence this method accepts Any reference but is
   * only defined for WhiskPackagePut, WhiskRulePut, and SequenceExec.
   *
   * It is plausible to lift these disambiguation below to a new trait which is
   * implemented by these types - however this will require exposing the Resource
   * type outside of the controller which is not yet desirable (although this could
   * cause further consolidation of the WhiskEntity and Resource types).
   *
   * @return Set of Resource instances if there are referenced entities.
   */
  def referencedEntities(reference: Any): Set[Resource] = {
    reference match {
      case WhiskPackagePut(Some(binding), _, _, _, _) =>
        Set(Resource(binding.namespace.toPath, Collection(Collection.PACKAGES), Some(binding.name.asString)))
      case r: WhiskRulePut =>
        val triggerResource = r.trigger.map { t =>
          Resource(t.path, Collection(Collection.TRIGGERS), Some(t.name.asString))
        }
        val actionResource = r.action map { a =>
          Resource(a.path, Collection(Collection.ACTIONS), Some(a.name.asString))
        }
        Set(triggerResource, actionResource).flatten
      case e: SequenceExec =>
        e.components.map { c =>
          Resource(c.path, Collection(Collection.ACTIONS), Some(c.name.asString))
        }.toSet
      case e: SequenceExecMetaData =>
        e.components.map { c =>
          Resource(c.path, Collection(Collection.ACTIONS), Some(c.name.asString))
        }.toSet
      case _ => Set()
    }
  }
}
