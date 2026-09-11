/*
 * Copyright 2017 ABSA Group Limited
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

package za.co.absa.spline.example.batch;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import za.co.absa.spline.agent.AgentConfig;
import za.co.absa.spline.harvester.SparkLineageInitializer;
import za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher;
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener;

import java.io.File;
import java.util.Arrays;

public class JavaExampleJob_Files {

    public static void main(String[] args) {



        final SparkSession session = SparkSession.builder()
            .appName("java example app")
            .master("local[*]")
            .config("spark.driver.host", "localhost")
            .config("spark.spline.mode", "ENABLED")  // ✅ Enable Spline
            .config("spark.spline.lineageDispatcher", "hdfs")  // ✅ Use HDFS dispatcher
            .config("spark.spline.lineageDispatcher.hdfs.className", "za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher") // 🔥 Fix: Define HDFS Dispatcher
            .config("spark.spline.lineageDispatcher.hdfs.directory", "file:///C:/spline")  // ✅ Store lineage files in local storage
            .config("spark.spline.lineageDispatcher.hdfs.fileNamePrefix", "lineage_")  // ✅ File naming pattern
            .getOrCreate();



// Enable Spline Tracking
        AgentConfig splineConfig = AgentConfig.builder().build();
        SparkLineageInitializer.enableLineageTracking(session, splineConfig);
        System.out.println("Spline Listeners: " + splineConfig.getConfigurationListeners().size());

        System.out.println("Using HDFSLineageDispatcher from: " +
            HDFSLineageDispatcher.class.getProtectionDomain().getCodeSource().getLocation());






        System.out.println("Spline Mode: " + session.conf().get("spark.spline.mode", "NOT SET"));
        System.out.println("Spline HDFS: " + session.conf().get("spark.spline.lineageDispatcher.hdfs.directory", "NOT SET"));


        // Explicitly enable Spline lineage tracking
        // This step is optional - see https://github.com/AbsaOSS/spline-spark-agent#programmatic-initialization



        // Sample Spark Job
        session.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data/input/batch/wikidata.csv")
            .as("source")
            .write()
            .mode(SaveMode.Overwrite)
            .csv("data/output/batch/java-sample3.csv");

//        printLineageFiles("/tmp/spline");

    }

    private static void printLineageFiles(String directoryPath) {
        File dir = new File(directoryPath);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                System.out.println("✅ Lineage Files Generated:");
                Arrays.stream(files).forEach(file -> System.out.println(" - " + file.getAbsolutePath()));
            } else {
                System.out.println("⚠️ No lineage files found in " + directoryPath);
            }
        } else {
            System.out.println("❌ Lineage directory does not exist: " + directoryPath);
        }
    }


}
