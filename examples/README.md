## Run Spline examples

1. Make sure the Spline Producer instance is running ([see instructions](https://absaoss.github.io/spline/#start-spline-server))

2. Download the Spline source code from GitHub and switch to the `examples` directory
    ```shell script
    git clone git@github.com:AbsaOSS/spline-spark-agent.git
    cd spline-spark-agent
    mvn install -DskipTests
    cd examples
    ```

#### Python (PySpark)

1. Execute `pyspark` with a _Spline Spark Agent Bundle_ corresponding to the _Spark_ and _Scala_ versions in use:
    ```shell script
      pyspark \
        --packages za.co.absa.spline.agent.spark:spark-3.1-spline-agent-bundle_2.12:0.6.1 \
        --conf spark.sql.queryExecutionListeners=za.co.absa.spline.harvester.listener.SplineQueryExecutionListener \
        --conf spark.spline.producer.url=http://localhost:8080/producer
    ```
   In this example we used a so-called _codeless_ initialization method,
   e.i. the one that requires no changes in your Spark application code.

   Alternatively you can enable Spline manually by calling the `SparkLineageInitializer.enableLineageTracking()` method.
   See [python_example.py](src/main/python/python_example.py)

2. Execute your PySpark code as normal.

#### Spark Shell

Same as `pyspark` example above, but use `spark-shell` command instead.

#### Scala / Java

###### Build the project

```shell script
# to build it with the default Spark version, simply run
mvn install

# or, if you want to specify a concrete Spark version from the 3.x series (i.e. `3.0`, `3.1` etc.)
mvn install -Pspark-3.1

# or, if you want to specify a concrete Spark version from the 2.x series (`2.2`, `2.3`, `2.4` only)
# 1. switch the project to Scala 2.11 mode
mvn scala-cross-build:change-version -Pscala-2.11
# 2. then run Maven build with the `-Pspark-xxx` profile as above
mvn install -Pspark-2.4
```

###### Run the examples

```shell script
# Execute all available examples
./run.sh --all -Dspline.producer.url=http://localhost:8080/producer

# Execute individual example class
./run.sh -Dspline.producer.url=http://localhost:8080/producer za.co.absa.spline.example.XXX
```

To add JVM options

```shell script
./run.sh -jvm_opt1=xxx -jvm_opt2=yyy ... class.to.run.ClassName
# or, if you run all examples, the '--all' argument should go first.
./run.sh --all -jvm_opt1=xxx -jvm_opt2=yyy ...
```

#### Examples source code

- [Scala](src/main/scala/za/co/absa/spline/example/)
- [Java](src/main/java/za/co/absa/spline/example/)
- [Python](src/main/python/)
- [Shell script](src/main/shell/) - custom, non-Spark example, using REST API

## Run Spline examples using docker image

Recommended docker settings: `cpu=2`, `memory=4096M`

 ```shell script
docker run --rm -e "SPLINE_PRODUCER_URL=http://localhost:8080/producer" absaoss/spline-spark-agent
 ```

Available environment variables:

| Variable name          | Description                                                                                                  |
|------------------------|--------------------------------------------------------------------------------------------------------------|
| SPLINE_PRODUCER_URL    | Spline Producer REST API endpoint URL                                                                        |
| SPLINE_MODE            | (see [Spline mode](../README.md#properties))                                                                 |
| DISABLE_SSL_VALIDATION | If `true`, disables validation of the server SSL certificate in the `HttpLineageDispatcher`                  |
| HTTP_PROXY_HOST        | (see [Java Networking and Proxies](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html)) |
| HTTP_PROXY_PORT        | (see [Java Networking and Proxies](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html)) |
| HTTP_NON_PROXY_HOSTS   | (see [Java Networking and Proxies](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html)) |

(The default values can be seen in the respective [Dockerfile](../Dockerfile))

---

    Copyright 2019 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
