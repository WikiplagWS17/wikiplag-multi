package de.htwberlin.f4.wikiplag.rest.launcher

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler}
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

/** Used to start the jetty app in standalone mode.
  *
  * @see http://scalatra.org/guides/2.4/deployment/standalone.html test for more information.
  */
object JettyLauncher {
  /**
    * The root path of the application. All other paths specified in the services are applied after this one.
    */
  val ROOT_PATH: String = "/"

  /**
    * The default port on which the REST-API will be launced if no port is specified.
    */
  val DEFAULT_PORT: Int = 8080

  /** The REST-API Application entry point.
    *
    * @param args not used.
    */
  def main(args: Array[String]) {
    //initialize a new server with the given port
    val port = if (System.getenv("PORT") != null) System.getenv("PORT").toInt else DEFAULT_PORT
    val server = new Server(port)

    //initialize the context
    val context = new WebAppContext()
    context.setContextPath(ROOT_PATH)
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")
    //bind the context to the server
    server.setHandler(context)

    //start the server
    server.start()
    server.join()
  }
}
