ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "com.example"

lazy val settings = Seq(
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
  libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
  )
)

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.7"

lazy val root = (project in file("."))
  .settings(
    name := "gensort",
    settings
  )
  .aggregate(master, worker)

lazy val master = (project in file("./master"))
  .settings(
    name := "master",
    settings,
    // mainClass in assembly := Some("dpsort.master.Main"),
    libraryDependencies += scalaTest % Test
  )

lazy val worker = (project in file("./worker"))
  .settings(
    name := "worker",
    settings
  )
