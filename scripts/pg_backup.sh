#!/usr/bin/env sh
set -eux

# Configurable via env:
: "${PG_HOST:?Need PG_HOST}"
: "${PG_PORT:?Need PG_PORT}"
: "${PG_USER:?Need PG_USER}"
: "${PG_DB:?Need PG_DB}"
: "${BACKUP_DIR:=/backups}"
: "${RETENTION_DAYS:=7}"

timestamp() {
  date +%F_%H%M%S
}

# Dump & gzip
outfile="${BACKUP_DIR}/${PG_DB}_$(timestamp).sql.gz"
pg_dump \
  --host="$PG_HOST" \
  --port="$PG_PORT" \
  --username="$PG_USER" \
  --format=custom \
  "$PG_DB" \
| gzip > "$outfile"

# Cleanup old
find "$BACKUP_DIR" -type f -name "${PG_DB}_*.sql.gz" \
  -mtime +"$RETENTION_DAYS" -delete

# (Optional) Upload to S3 if env AWS_* are set
if [ -n "${AWS_S3_BUCKET:-}" ]; then
  AWS_DEFAULT_OUTPUT=text aws s3 cp "$outfile" "s3://${AWS_S3_BUCKET}/"
fi