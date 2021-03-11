FROM maven:3.6.3-openjdk-8
COPY . /opt/spline-spark-agent

RUN cd /opt/spline-spark-agent && mvn install -DskipTests

ENV WORKDIR=/opt/spline-spark-agent/examples
ENV SPLINE_PRODUCER_URL=http://spline.spline:8080/producer

WORKDIR $WORKDIR

CMD mvn test -P examples -D spline.producer.url=$SPLINE_PRODUCER_URL