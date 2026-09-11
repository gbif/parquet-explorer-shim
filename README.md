# parquet-explorer-shim

A Spring Boot service that lets [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) (or server-side DuckDB) run SQL directly against GBIF DWC-DP datapackages — one dataset + crawl attempt at a time — without downloading them first. It has two jobs:

1. **WebHDFS range shim** (`/hdfs/**`) — translates HTTP `Range` requests (what DuckDB's `httpfs` extension speaks) into WebHDFS's `offset`/`length` query params (what WebHDFS speaks), so a browser or server-side DuckDB can read individual parquet row groups out of HDFS instead of fetching whole files.
2. **Datapackage query API** (`/dataset/**`) — given a dataset UUID and crawl attempt, resolves every resource in that attempt's `datapackage.json` to a DuckDB view (parquet or CSV/TSV, local disk or HDFS via the shim above) and runs read-only SQL across them, including joins between resources.

A standalone CLI (`DwcDpParquetConverter`) is also included for batch-converting a datapackage's TSV/CSV resources to parquet ahead of time.

## Endpoints

### `POST /dataset/{uuid}/dwcdp/{attempt}/query`
```json
{ "sql": "SELECT * FROM occurrence LIMIT 10" }
```
Runs `sql` against dataset `{uuid}`'s datapackage at crawl `{attempt}`. Every resource declared in that attempt's `datapackage.json` becomes a DuckDB view named after the resource (e.g. a resource named `occurrence` is queryable as `FROM occurrence`), so SQL can join across resources directly. Looks the datapackage up in two repositories, in order:

1. **Local** — `local.base-path/<uuid>/<attempt>/`, read directly off disk.
2. **HDFS** — `hdfs.dwcdp-root-path/<uuid>/<attempt>/`, read through this service's own `/hdfs` shim (below) via a self-loopback — used only when the dataset/attempt isn't found locally.

The two repositories are independent; this endpoint doesn't merge them, it tries local first and falls back whole-package to HDFS.

### `POST /dataset/{uuid}/dwcdp/query`
Same as above, but without an attempt number — resolves it itself by calling GBIF's pipelines history API (`GET {gbif.api-base-url}/pipelines/history/{uuid}/lastSuccessfulAttempt`) for the dataset's last successful crawl attempt, then runs the query exactly as the `{attempt}`-taking endpoint does.

SQL is restricted server-side to `SELECT` / `WITH` / `DESCRIBE` / `SHOW` / `EXPLAIN` / `SUMMARIZE`, single statement only — see `DuckDbQueryService`.

### `GET`/`HEAD /hdfs/**`
Range-request proxy in front of WebHDFS, e.g. `GET /hdfs/data/foo.parquet` with a `Range: bytes=0-1023` header. Used internally by the query endpoints above (via `shim.self-base-url`) to read HDFS-hosted datapackages, and can also be pointed at directly by a browser-side DuckDB-WASM instance. Only paths under `hdfs.allowed-prefixes` are served.

### Operational endpoints
`/actuator/health` and `/actuator/prometheus` (Spring Boot Actuator + Micrometer).

## Configuration

All configuration is in `src/main/resources/application.yml`, overridable via environment variables (standard Spring Boot relaxed binding, e.g. `HDFS_WEBHDFS_BASE_URL`).

| Key | Purpose |
|---|---|
| `hdfs.webhdfs-base-url` | WebHDFS endpoint the `/hdfs` shim proxies to. |
| `hdfs.allowed-prefixes` | Comma-separated HDFS path prefixes the shim is allowed to serve — the access boundary for a public-facing deployment. |
| `hdfs.namenoderpcaddress` | Required by Stackable WebHDFS for NameNode routing; omit if your cluster doesn't need it. |
| `hdfs.dwcdp-root-path` | Root under which converted datapackages live in HDFS, as `<uuid>/<attempt>/`. Fallback repository for the query endpoints; leave blank to disable the HDFS fallback entirely. |
| `local.base-path` | Root under which converted datapackages live on local disk (typically an NFS mount), as `<uuid>/<attempt>/`. Checked before HDFS. |
| `gbif.api-base-url` | Base URL of GBIF's registry/pipelines API, used only to resolve `lastSuccessfulAttempt` for `POST /dataset/{uuid}/dwcdp/query`. |
| `shim.cors-allowed-origin` | Comma-separated allowed CORS origins (see `CorsConfig`). |
| `shim.webhdfs-user` | `user.name` passed to WebHDFS simple auth. |
| `shim.self-base-url` | This service's own reachable base URL, used to build the loopback `/hdfs/...` source URL the query endpoints read HDFS-fallback datapackages through. |
| `duckdb.query.pool-size` | Pre-warmed DuckDB JDBC connections kept ready for queries. |
| `duckdb.query.max-rows` | Rows buffered into a query response before truncating (does not limit query execution — add `LIMIT` in SQL for that). |
| `duckdb.query.timeout-seconds` | Passed to `Statement#setQueryTimeout`; a safety net, not a hard guarantee (duckdb_jdbc's native query interruption support is partial). |
| `duckdb.query.borrow-timeout-seconds` | How long a request waits for a free pooled connection before failing with `503`. |

## Running locally

```
mvn spring-boot:run
```

Defaults to port `8080`. Point `local.base-path` at a directory containing at least one `<uuid>/<attempt>/datapackage.json`, or configure `hdfs.dwcdp-root-path` + `hdfs.webhdfs-base-url` against a reachable WebHDFS cluster.

```
mvn test
```

## Building

```
mvn package
```
Produces an executable jar via `spring-boot-maven-plugin`. Run with `java -jar target/parquet-explorer-shim-*.jar --spring.config.location=/path/to/application.yml`.

## Batch parquet conversion (CLI)

```
java -cp target/classes:<deps> org.gbif.parquetexplorer.shim.convert.DwcDpParquetConverter <packageDir> [outputDir] [--force]
```
Converts a datapackage's TSV/CSV resources to parquet ahead of time, using column types from the resource's Frictionless Table Schema (`datapackage.json`) rather than re-inferring them. Run against `local.base-path`, not WebHDFS. Writes `<resource>.parquet` per resource plus a sibling `datapackage.json` with paths/formats rewritten to point at the parquet output.

## Deployment

The service itself is just the Spring Boot jar (see Building, above) — run it behind whatever reverse proxy/LB terminates TLS and CORS-fronts your frontend. `src/main/resources/dwcdp-explorer.conf.template` is a ready-made Apache VirtualHost template for that reverse proxy, covering the two things it needs to do:

- Serve a built frontend as static files, with an SPA fallback to `index.html`.
- Reverse-proxy `/dataset/**` (the only path the frontend calls — see `VITE_SHIM_BASE_URL` below) to this service.

It ships on the classpath (packaged inside the jar under `dwcdp-explorer.conf.template`) rather than as a loose repo file, so it travels with a given build/release — extract it with e.g. `unzip -p target/parquet-explorer-shim-*.jar dwcdp-explorer.conf.template` or `jar xf`, or just read it straight from `src/main/resources/` in this repo.

### Install

1. **Build and run the shim** (see Building, above) somewhere reachable from your proxy host, e.g. `cli1.gbif-dev.org:10000`.

2. **Build the frontend**, pointed at this shim and at GBIF's catalog API:
   ```
   cd frontend
   VITE_SHIM_BASE_URL=/ VITE_GBIF_API_BASE_URL=https://api.gbif-dev.org/v1 npm run build
   ```
   `VITE_SHIM_BASE_URL=/` makes the frontend call the shim as a same-origin relative path (e.g. `POST /dataset/{uuid}/dwcdp/query`) — the proxy config below only forwards that one path, wherever the shim actually runs. `VITE_GBIF_API_BASE_URL` should match the same environment as the shim's `local.base-path` / `hdfs.dwcdp-root-path` / `gbif.api-base-url`, or the catalog will list datasets the shim can't find.

3. **Copy the build output** to the directory Apache will serve as static files:
   ```
   sudo mkdir -p /var/www/dwcdp-explorer
   sudo cp -r frontend/dist/* /var/www/dwcdp-explorer/
   ```

4. **Render the Apache template** by substituting its placeholders — `SERVER_NAME`, `DOCUMENT_ROOT`, `BACKEND_HOST`, `BACKEND_PORT`, and (only if using the dedicated-port or HTTPS variant in the template) `LISTEN_PORT`, `SSL_CERT_FILE`, `SSL_KEY_FILE`:
   ```
   export SERVER_NAME=dwcdp-explorer.labs.gbif.org
   export DOCUMENT_ROOT=/var/www/dwcdp-explorer
   export BACKEND_HOST=cli1.gbif-dev.org
   export BACKEND_PORT=10000

   envsubst '${SERVER_NAME} ${DOCUMENT_ROOT} ${BACKEND_HOST} ${BACKEND_PORT}' \
       < dwcdp-explorer.conf.template > dwcdp-explorer.conf
   ```
   Pass the exact variable list to `envsubst` as shown — otherwise it will also try to expand `${APACHE_LOG_DIR}`, which the template deliberately leaves untouched for Apache itself to resolve at runtime.

5. **Install and enable the site**:
   ```
   sudo cp dwcdp-explorer.conf /etc/apache2/sites-available/dwcdp-explorer.conf
   sudo a2enmod proxy proxy_http headers rewrite
   sudo a2ensite dwcdp-explorer
   sudo systemctl reload apache2
   ```

The template's own header comment repeats this sequence and documents the commented-out HTTPS and dedicated-port variants (`OPTION A` with TLS, `OPTION B`) for when a subdomain + plain HTTP isn't the right fit.

## Security notes

- The `/hdfs` shim's only access control is the `hdfs.allowed-prefixes` check — pair it with network-level restrictions (e.g. only a proxy/LB can reach this service) before exposing it broadly.
- The query endpoints accept arbitrary SQL, restricted to read-only statement types by an allowlist + denylist in `DuckDbQueryService`. DuckDB itself has real filesystem/network access from within the process, so treat this as no less sensitive than the WebHDFS shim it sits next to.
- Dataset UUIDs and crawl attempt numbers are strictly parsed/validated before use in any filesystem or HDFS path (see `parseUuid`, `validateHdfsPath`), which also rules out path traversal via those inputs.

## Related

A companion React frontend (dataset browser + SQL explorer UI) exists alongside this service at the time of writing and talks to it over `/dataset/**`; see `dwcdp-explorer.conf` for how the two are wired together behind Apache.
