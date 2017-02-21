package com.stripe.herringbone

import com.stripe.herringbone.flatten.{FlatConverter, ParquetFlatConf, ParquetFlatMapper, TypeFlattener}
import com.stripe.herringbone.util.ParquetUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util._
import org.apache.parquet.example.data._
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example._

class FlattenMapper extends ParquetFlatMapper[Group] {
  def valueOut(value: Group) = {
    FlatConverter.flattenGroup(value, flattenedSchema, separator, renameId)
  }
}

class FlattenJob extends Configured with Tool {
  override def run(args: Array[String]) = {
    val conf = new ParquetFlatConf(args)
    val fs = FileSystem.get(getConf)
    val inputPath = new Path(conf.inputPath())
    val outputPathString = conf.outputPath.get.getOrElse(conf.inputPath().stripSuffix("/").concat("-flat"))
    val outputPath = new Path(outputPathString)
    val previousPath = conf.previousPath.get.map{new Path(_)}

    val separator = conf.separator()
    getConf.set(ParquetFlatMapper.SeparatorKey, separator)

    val renameId = conf.renameId()
    getConf.set(ParquetFlatMapper.RenameIdKey, renameId.toString)

    if (fs.exists(outputPath)) {
      println(s"Deleting existing $outputPath")
      fs.delete(outputPath, true)
    }

    val flattenedSchema = TypeFlattener.flatten(
      ParquetUtils.readSchema(inputPath),
      previousPath.map { ParquetUtils.readSchema(_) },
      separator,
      renameId
    )

    val jobName = "flatten " + conf.inputPath() + " -> " + outputPathString
    val job = new Job(getConf, jobName)

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    ExampleOutputFormat.setSchema(job, flattenedSchema)
    ParquetInputFormat.setReadSupportClass(job, classOf[GroupReadSupport])

    job.setInputFormatClass(classOf[CompactGroupInputFormat]);
    job.setOutputFormatClass(classOf[ExampleOutputFormat])
    job.setMapperClass(classOf[FlattenMapper])
    job.setJarByClass(classOf[FlattenJob])
    job.getConfiguration.setBoolean("mapreduce.job.user.classpath.first", true)
    job.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, false)
    job.getConfiguration.setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, false);
    job.setNumReduceTasks(0)

    if (job.waitForCompletion(true)) 0 else 1
  }
}

object FlattenJob {
  def main(args: Array[String]) = {
    val result = ToolRunner.run(new Configuration, new FlattenJob, args)
    System.exit(result)
  }
}
