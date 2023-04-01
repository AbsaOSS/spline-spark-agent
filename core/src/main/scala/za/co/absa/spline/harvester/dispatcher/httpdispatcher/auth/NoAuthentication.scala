package za.co.absa.spline.harvester.dispatcher.httpdispatcher.auth

import org.apache.commons.configuration.Configuration
import scalaj.http.HttpRequest

object NoAuthentication extends Authentication {
  override def authenticate(httpRequest: HttpRequest, authConfig: Configuration): HttpRequest = httpRequest
}
