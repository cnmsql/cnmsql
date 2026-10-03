#!/usr/bin/env bash
#
# slowlog-fixtures.sh — capture the slow log of every supported instance image,
# the fixtures pkg/management/mysql/slowlog parses in its tests.
#
# Each image starts mysqld with long_query_time=0, runs a fixed workload, and
# the log is copied out twice: plain, then with the engine's verbose slow log
# options (log_slow_verbosity, plus log_slow_extra on MySQL).
#
# Usage:
#   hack/slowlog-fixtures.sh            regenerate every fixture
#
# Environment:
#   INSTANCE_IMAGE_REPO          default ghcr.io/cnmsql/cnmsql-instance
#   MARIADB_INSTANCE_IMAGE_REPO  default ghcr.io/cnmsql/cnmsql-mariadb-instance

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/pkg/management/mysql/slowlog/testdata"
MYSQL_REPO="${INSTANCE_IMAGE_REPO:-ghcr.io/cnmsql/cnmsql-instance}"
MARIADB_REPO="${MARIADB_INSTANCE_IMAGE_REPO:-ghcr.io/cnmsql/cnmsql-mariadb-instance}"

# capture runs inside the image, as the image's unprivileged user.
read -r -d '' capture <<'SCRIPT' || true
set -u
D=/tmp/data; R=/var/run/mysqld; LOG=$R/mysqld-slow.log; mkdir -p $D $R
if command -v mariadbd >/dev/null; then
  S=mariadbd; C=mariadb
  mariadb-install-db --no-defaults --datadir=$D --auth-root-authentication-method=normal >/dev/null 2>&1
  VERB="--log-slow-verbosity=query_plan,explain,innodb"
else
  S=mysqld; C=mysql
  mysqld --no-defaults --initialize-insecure --datadir=$D >/dev/null 2>&1
  VERB="--log-slow-verbosity=full --log-slow-extra=ON"
fi
SOCK=$R/mysqld.sock
q() { $C -uroot -S $SOCK "$@" >/dev/null 2>&1; }
run() {
  $S --no-defaults --datadir=$D --socket=$SOCK --skip-networking --log-error=/tmp/err.log --log-output=FILE \
    --slow-query-log=ON --slow-query-log-file=$LOG --long-query-time=0 --log-slow-admin-statements=ON "$@" \
    >/dev/null 2>&1 &
  P=$!
  for _ in $(seq 60); do q -e "SELECT 1" && break; sleep 0.5; done
  : > $LOG
}
workload() {
  q -e "CREATE DATABASE IF NOT EXISTS shop; CREATE TABLE IF NOT EXISTS shop.t (id INT PRIMARY KEY, v VARCHAR(20)); REPLACE INTO shop.t VALUES (1,'a'),(2,'b');"
  q shop -e "SELECT * FROM t WHERE v = 'a'"
  printf "SELECT id,\n       v\n  FROM shop.t\n WHERE id > 0;\n" | q
  q -e "SELECT 'it''s; # not a header', SLEEP(0.01)"
  q -e "SELECT 1; SELECT 2"
  q -e "OPTIMIZE TABLE shop.t"
}
run; workload; sleep 0.5; cp $LOG /out/plain.log; q -e "SHUTDOWN"; wait $P
run $VERB; workload; sleep 0.5; cp $LOG /out/verbose.log; q -e "SHUTDOWN"; wait $P
SCRIPT

images=(
  "mysql-8.0=${MYSQL_REPO}:8.0"
  "mysql-8.4=${MYSQL_REPO}:8.4"
  "mysql-9.7=${MYSQL_REPO}:9.7"
  "mariadb-10.11=${MARIADB_REPO}:10.11"
  "mariadb-11.4=${MARIADB_REPO}:11.4"
  "mariadb-12.3=${MARIADB_REPO}:12.3"
)
for entry in "${images[@]}"; do
  name="${entry%%=*}"
  image="${entry#*=}"
  mkdir -p "${OUT}/${name}"
  chmod 777 "${OUT}/${name}"
  docker run --rm -v "${OUT}/${name}:/out" --entrypoint bash "${image}" -c "${capture}"
  chmod 755 "${OUT}/${name}"
  echo "${name}: $(wc -l < "${OUT}/${name}/plain.log") plain, $(wc -l < "${OUT}/${name}/verbose.log") verbose lines"
done
