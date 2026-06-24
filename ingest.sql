COPY (
    SELECT *
    FROM read_csv(
        $source_path,
        header      = $header,
        delim       = $delimiter,
        all_varchar = $all_varchar
    )
)
TO $dest_path
(FORMAT CSV, HEADER $header, DELIMITER $delimiter);
