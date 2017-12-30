package de.htwberlin.f4.wikiplag.rest

import org.scalatra.test.scalatest._
import org.scalatest.FunSuiteLike

class WikiplagServletTests extends ScalatraSuite with FunSuiteLike {

  addServlet(classOf[WikiplagServlet], "/*")

  test("GET / on WikiplagServlet should return status 200"){
    get("/"){
      status should equal (200)
    }
  }
}
