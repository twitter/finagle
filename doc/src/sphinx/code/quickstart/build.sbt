lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.twitter",
      scalaVersion := "2.12.12",
      version      := "1.0"
    )),
    name := "quickstart",
    libraryDependencies ++= Seq(
      "com.twitter" %% "finagle-http" % "21.8.0"
    )
  )
