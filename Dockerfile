# Run unit tests and build jar file
FROM mozilla/sbt:8u232_1.3.7 AS build
WORKDIR /app
COPY . .
RUN sbt test && sbt package

# Package jar file and set spark-submit execute string
FROM cjonesy/docker-spark:2.4.3 AS release
COPY --from=build /app/target/scala-2.11/nasalogs_2.11-0.0.1.jar /
CMD [ \
    "spark-submit", \
    "--master=local", \
    "--class=com.pavandendi.nasalogs.NasaLogs", \
    "/nasalogs_2.11-0.0.1.jar", \
    "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz", \
    "3" \
    ]
