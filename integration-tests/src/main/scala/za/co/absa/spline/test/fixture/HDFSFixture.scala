/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.test.fixture

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertion, AsyncTestSuite}

import java.io.File
import scala.concurrent.Future
trait HDFSFixture {
  this: AsyncTestSuite =>
//
//  def usingResource[R](resource: R)(testBody: R => Future[Assertion])(implicit releasable: Releasable[R]): Future[Assertion] = {
//    testBody(resource).andThen { case _ => releasable.release(resource) }
//  }


  def withHdfs(testBody: MiniDFSCluster => Future[Assertion]): Future[Assertion] = {
    val hdfsCluster = createMiniCluster()
    testBody(hdfsCluster).andThen { case _ => hdfsCluster.shutdown() }
  }


//  override def beforeAll(): Unit = {
//    super.beforeAll()
//
//    startMiniCluster()
//    copyResourcesToHdfs()
//    addClusterConfigToSpark()
//  }

  def createMiniCluster(): MiniDFSCluster = {
    val baseDir = new File("./target/hdfs/")
    FileUtil.fullyDelete(baseDir)

    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    conf.setBoolean("dfs.webhdfs.enabled", true)
    val hdfsCluster = new MiniDFSCluster.Builder(conf)
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()

    hdfsCluster.waitClusterUp()
    hdfsCluster
  }

  def copyResourcesToHdfs(hdfsCluster: MiniDFSCluster): Unit = {
    hdfsCluster
      .getFileSystem()
      .copyFromLocalFile(new Path("data/cities.csv"), new Path("/"))
  }

  def addClusterConfigToSpark( spark: SparkSession, hdfsCluster: MiniDFSCluster): Unit = {
    spark.sparkContext.hadoopConfiguration.addResource(hdfsCluster.getFileSystem().getConf)
  }


  //hdfsCluster.getFileSystem.

//  override def afterAll(): Unit = {
//    hdfsCluster.shutdown()
//    super.afterAll()
//  }
}
