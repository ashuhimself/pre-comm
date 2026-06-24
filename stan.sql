-- Copy validated ingest file to standardise bucket,
-- Only runs when schema_validation.sql passed (0 invalid rows).
COPY (
    SELECT * FROM read_csv(
        $source_path,
        header        = $header,
        delim         = $delimiter,
        all_varchar   = $all_varchar,
        ignore_errors = $ignore_errors
    )
)
TO $dest_path
(FORMAT CSV, HEADER $header, DELIMITER $delimiter);