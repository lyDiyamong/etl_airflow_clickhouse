FROM metabase/metabase:v0.52.2.3

ENV METABASE_CLICKHOUSE_DRIVER_VERSION=1.51.0

RUN mkdir -p mb/plugins && cd mb
RUN curl -L -o plugins/ch.jar https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/$METABASE_CLICKHOUSE_DRIVER_VERSION/clickhouse.metabase-driver.jar