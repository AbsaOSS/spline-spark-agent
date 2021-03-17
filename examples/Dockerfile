FROM maven:3.6.3-openjdk-8
COPY . /opt/spline-spark-agent

ENV SPLINE_PRODUCER_URL=http://spline.spline:8080/producer

RUN cd /opt/spline-spark-agent && \
    mvn install -DskipTests && \
    cd ./examples && \
    mvn test -P examples -D spline.producer.url=$SPLINE_PRODUCER_URL

ENV WORKDIR=/opt/spline-spark-agent/examples


WORKDIR $WORKDIR

CMD mvn test -P examples -D spline.producer.url=$SPLINE_PRODUCER_URL 