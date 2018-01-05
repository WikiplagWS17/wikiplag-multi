import de.htwberlin.f4.wikiplag.rest._
import org.scalatra._
import javax.servlet.ServletContext

import de.htwberlin.f4.wikiplag.rest.servlets.WikiplagServlet

/** Bootstrap class. Called in the web.xml */
class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    //the path to our service
    context.mount(new WikiplagServlet, "/*")
  }
}
