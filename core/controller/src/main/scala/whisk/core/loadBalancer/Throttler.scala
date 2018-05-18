package whisk.core.loadBalancer

import whisk.common.TransactionId
import whisk.core.entitlement.{Privilege, Resource}
import whisk.core.entity.Identity

import scala.collection.immutable.Set
import scala.concurrent.Future

trait Throttler {

  def check(user: Identity, right: Privilege, resources: Set[Resource])(implicit transid: TransactionId): Future[Unit]

}
