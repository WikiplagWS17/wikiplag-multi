import org.scalatra._
import javax.servlet.ServletContext

import de.htwberlin.f4.wikiplag.rest.servlets.WikiplagServlet

/** Bootstraps the servlets to the application.
  *
  * @note This class can be specified in the web.xml file, but if isn't (which is currently the case)
  *       scalatra looks for a class named ScalatraBootstrap in the default package and starts it.
  */
class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    //bind the WikiPlag  servlet to the given path. All requests sent to this path will be handled by the servlet
    context.mount(new WikiplagServlet, "/wikiplag/rest/*")
  }
}
