/* 
  This is information about our project build
  Scala version = 2.12.17
  Team name = Red
*/
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "postech.team.Red"

/*
  This is basic setting related to gRPC communication
*/
lazy val settings = Seq(
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
  libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
  )
)

lazy val commonAssemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x                             => MergeStrategy.first
  }
)

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.7"

/*
  From this line, we defined project directory structures.
  root is root of the project
*/
lazy val root = (project in file("."))
  .settings(
    name := "gensort",
    settings,
    commonAssemblySettings
  )
  .aggregate(master, worker)
  .dependsOn(master, worker)

lazy val master = (project in file("master"))
  .settings(
    name := "master",
    settings,
    commonAssemblySettings,
    assembly / mainClass := Some("gensort.master.MasterWorkerServer"),
    libraryDependencies += scalaTest % Test
  )

lazy val worker = (project in file("worker"))
  .settings(
    name := "worker",
    settings,
    commonAssemblySettings
  )
  .dependsOn(master)

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    commonAssemblySettings,
    settings
  )

