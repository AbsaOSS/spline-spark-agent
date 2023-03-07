package za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest

import java.time.{Duration, Instant}

import scalaj.http.{Base64, HttpRequest}

import scala.collection.mutable

trait Authentication {
  def createRequest(httpRequest: HttpRequest,authentication: Map[String,String]):HttpRequest
}

case class ClientCredentialsAuthentication(authentication: Map[String, String]) extends Authentication {
  private val tokenCache: mutable.Map[String, (String, Instant)] = mutable.Map.empty[String, (String, Instant)]
  private val clientId: String = ""
  private val clientSecret: String = ""
  private val tokenUrl: String = ""
  private val grantType: String = ""
  private val scope: String = ""

  private def isTokenExpired(expirationTime: Instant): Boolean = {
    Instant.now().isAfter(expirationTime.minusSeconds(300) )
  }

  private def getToken(clientId: String, clientSecret: String, tokenUrl: String,scope: String): String = {
    val cachedToken= tokenCache.get("token")
    if (cachedToken.isDefined && !isTokenExpired(cachedToken.get._2)) {
      // Token found in cache and not expired, return it
      cachedToken.get._1
    } else {
      val resp = scalaj.http.Http(tokenUrl)
        .postForm(Seq(
          "grant_type" -> "client_credentials",
          "client_id" -> clientId,
          "client_secret" -> clientSecret,
          "scope" -> scope))
        .asString

      val jsonResp = scala.util.parsing.json.JSON.parseFull(resp.body)
      jsonResp match {
        case Some(map: Map[String, Any]) =>
          val token = map.getOrElse("access_token", "").toString
          if (token.nonEmpty) {
            val expirationTime = Instant.now().plus(Duration.ofSeconds(map.getOrElse("expires_in", "0").toString.toLong))
            tokenCache.put("token", (token, expirationTime))
          }
          token
        case _ =>
          throw new RuntimeException("Failed to retrieve token from response")

      }
    }
  }

  override def createRequest(httpRequest: HttpRequest, authentication: Map[String, String]): HttpRequest ={
    authentication.get("mode") match {
      case Some("enabled") => {
        if (authentication.contains("clientId") && authentication.contains("clientSecret") && authentication.contains("tokenUrl") && authentication.contains("scope")) {
          val clientId = authentication("clientId")
          val clientSecret = authentication("clientSecret")
          val tokenUrl = authentication("tokenUrl")
          val scope = authentication("scope")
          val token = getToken(clientId, clientSecret, tokenUrl, scope)
          httpRequest.header("Authorization", s"Bearer $token")
        } else {
          throw new IllegalArgumentException("Missing or wrong credentials")
        }
      }
      case _ => httpRequest
    }
  }
}

object AuthenticationFactory {
  def createAuthentication(authentication: Map[String, String]): Authentication = {
    authentication("grantType") match {
      case "client_credentials" => ClientCredentialsAuthentication(authentication)
      case _ => throw new IllegalArgumentException("Invalid grant type.")
    }
  }
}

