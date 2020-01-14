package avrohugger
package format
package specific

import format.abstractions.JavaTreehugger
import stores.ClassStore

import treehugger.forest._
import definitions._
import treehuggerDSL._

import java.io.{
  File,
  BufferedWriter,
  FileWriter,
  FileNotFoundException,
  IOException
}

import java.nio.file.{Files, Paths}

import org.apache.avro.{ Protocol, Schema }
import org.apache.avro.compiler.specific.SpecificCompiler

object SpecificJavaTreehugger extends JavaTreehugger {

  def asJavaCodeString(
    classStore: ClassStore,
    namespace: Option[String],
    schema: Schema): String = {

    def writeJavaTempFile(
      namespace: Option[String],
      schema: Schema,
      outDir: String): Unit = {
	    // Uses Avro's SpecificCompiler, which only compiles from files, thus we 
      // write the schema to a temp file so we can compile a Java enum from it.
	    val tempSchemaFile = File.createTempFile(outDir + "/" + schema.getName, ".avsc")
	    tempSchemaFile.deleteOnExit()
	    val out = new BufferedWriter(new FileWriter(tempSchemaFile))
	    out.write(schema.toString)
	    out.close()

	    val folderPath = {
	      if (namespace.isDefined) new File(outDir)
	      else new File(outDir) 
	    }
	    try { 
	      SpecificCompiler.compileSchema(tempSchemaFile, folderPath)
	    }     
	    catch {
	      case ex: FileNotFoundException =>
          sys.error("File not found:" + ex)
	      case ex: IOException =>
          sys.error("There was a problem using the file: " + ex)
	    }
	  }

    def deleteTemps(path: String) = {
    	import scala.collection.JavaConverters._
    	Files
	      .walk(Paths.get(path))
	      .iterator()
	      .asScala
	      .toList
	      .reverse
	      .map(_.toFile)
	      .foreach(_.delete())
    }

    // Avro's SpecificCompiler only writes files, but we need a string
    // so write the Java file and read
    val outDir = System.getProperty("java.io.tmpdir")
    writeJavaTempFile(namespace, schema, outDir)
    val tempPath = outDir + "/" + schema.getFullName.replace('.','/') + ".java"
    val tempFile = new File(tempPath)
    val fileContents = scala.io.Source.fromFile(tempPath)
    val schemaPackage = "package " + schema.getNamespace
    val updatedPackage = namespace match {
      case Some(ns) => "package " + ns
      case None => ""
    }
    val schemaNamespace = """\"namespace\":\"""" +schema.getNamespace+ """\","""
    val updatedNamespace = namespace match {
      case Some(ns) => """\"namespace\":\"""" + ns + """\","""
      case None => ""
    }
    val codeString = fileContents.mkString
      .replace(schemaPackage, updatedPackage)
      .replace(schemaNamespace, updatedNamespace)
    fileContents.close
    deleteTemps(tempPath)
    codeString
  }

}


