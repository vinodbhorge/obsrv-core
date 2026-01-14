FROM maven:3.9.4-eclipse-temurin-11-focal AS build-core
COPY . /app
RUN mvn clean install -DskipTests -f /app/pom.xml
# RUN mvn clean install -DskipTests -f /app/dataset-registry/pom.xml
# RUN mvn clean install -DskipTests -f /app/transformation-sdk/pom.xml

FROM maven:3.9.4-eclipse-temurin-11-focal AS build-pipeline
COPY --from=build-core /root/.m2 /root/.m2
COPY . /app
RUN mvn clean package -DskipTests -f /app/pipeline/pom.xml

FROM sanketikahub/flink:1.20-scala_2.12-java11 AS extractor-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/extractor/target/extractor-1.0.0.jar $FLINK_HOME/usrlib/

FROM sanketikahub/flink:1.20-scala_2.12-java11 AS preprocessor-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/preprocessor/target/preprocessor-1.0.0.jar $FLINK_HOME/usrlib/

FROM sanketikahub/flink:1.20-scala_2.12-java11 AS denormalizer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/denormalizer/target/denormalizer-1.0.0.jar $FLINK_HOME/usrlib/

FROM sanketikahub/flink:1.20-scala_2.12-java11 AS transformer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/transformer/target/transformer-1.0.0.jar $FLINK_HOME/usrlib/

FROM sanketikahub/flink:1.20-scala_2.12-java11 AS dataset-router-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/dataset-router/target/dataset-router-1.0.0.jar $FLINK_HOME/usrlib/

# unified image build
FROM sanketikahub/flink:1.20-scala_2.12-java11 AS unified-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/unified-pipeline/target/unified-pipeline-1.0.0.jar $FLINK_HOME/usrlib/

# # Lakehouse connector image build
# FROM sanketikahub/flink:1.17.2-scala_2.12-java11 AS lakehouse-connector-image
# USER flink
# RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
# RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.2/flink-s3-fs-hadoop-1.17.2.jar
# RUN wget https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink1.17-bundle/1.0.2/hudi-flink1.17-bundle-1.0.2.jar
# RUN mv flink-shaded-hadoop-2-uber-2.8.3-10.0.jar $FLINK_HOME/lib
# RUN mv flink-s3-fs-hadoop-1.17.2.jar $FLINK_HOME/lib
# RUN mv hudi-flink1.17-bundle-1.0.2.jar $FLINK_HOME/lib
# # RUN mkdir $FLINK_HOME/custom-lib
# COPY --from=build-pipeline /app/pipeline/hudi-connector/target/hudi-connector-1.0.0.jar $FLINK_HOME/lib

# cache indexer image build
FROM sanketikahub/flink:1.20-scala_2.12-java11 AS cache-indexer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=build-pipeline /app/pipeline/cache-indexer/target/cache-indexer-1.0.0.jar $FLINK_HOME/usrlib/