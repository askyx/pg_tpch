CREATE FUNCTION dbgen_internal(
  IN sf INT,
  IN gentable TEXT,
  IN max_rows int DEFAULT -1
) RETURNS INT AS 'MODULE_PATHNAME',
'dbgen_internal' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION dbgen(sf INT, max_rows INT DEFAULT -1) RETURNS TABLE(tab TEXT, row_count INT) AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT table_name, status, child FROM tpch.tpch_tables LOOP
        -- skip child tables
        IF rec.status <> 1 THEN
            row_count := dbgen_internal(sf, rec.table_name, max_rows);
            tab := rec.table_name;
            RETURN NEXT;
            IF rec.status = 2 THEN
                tab := rec.child;
                RETURN NEXT;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tpch_prepare() RETURNS BOOLEAN AS 'MODULE_PATHNAME',
'tpch_prepare' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpch_cleanup() RETURNS BOOLEAN AS $$
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM tpch.tpch_tables
    LOOP
        EXECUTE 'truncate ' || tbl;
    END LOOP;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tpch_queries(IN QID INT DEFAULT 0, OUT qid INT, OUT query TEXT) RETURNS SETOF record AS 'MODULE_PATHNAME',
'tpch_queries' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpch_run(
  IN QID INT DEFAULT 0,
  OUT id INT,
  OUT duration DOUBLE PRECISION,
  OUT Checked BOOLEAN
) RETURNS record AS 'MODULE_PATHNAME',
'tpch_runner' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpch_run_internal(IN QID INT, IN REPLACE BOOLEAN DEFAULT FALSE) RETURNS TABLE (
  ret_id INT,
  new_duration DOUBLE PRECISION,
  old_duration DOUBLE PRECISION,
  ret_checked BOOLEAN
) AS $$
DECLARE
    run_record record;
BEGIN
    SELECT id, duration, checked INTO run_record FROM tpch_run(QID);

    ret_id := run_record.id;
    new_duration := run_record.duration;
    ret_checked := run_record.checked;

    SELECT ec_duration INTO old_duration FROM tpch.tpch_query_stats WHERE ec_qid = run_record.id;

    IF NOT FOUND THEN
        INSERT INTO tpch.tpch_query_stats VALUES (run_record.id, run_record.duration, current_timestamp(6));
    ELSE
        IF REPLACE AND run_record.duration < old_duration THEN
            UPDATE tpch.tpch_query_stats
            SET ec_duration = run_record.duration, ec_recoed_time = current_timestamp(6)
            WHERE ec_qid = run_record.id;
        END IF;
    END IF;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tpch(VARIADIC queries INT [] DEFAULT '{0}' :: INT []) RETURNS TABLE (
  "Qid" CHAR(2),
  "Stable(ms)" TEXT,
  "Current(ms)" TEXT,
  "Diff(%)" TEXT,
  "Result" TEXT
) AS $$
DECLARE
    run_record record;
    sum_statble numeric := 0;
    sum_run numeric := 0;
    query_count int := array_length(queries, 1);
    run_all boolean := false;
    i int;
BEGIN
    IF query_count = 1 AND queries[1] = 0 THEN
        run_all := true;
        query_count := 99;
    END IF;

    FOR i IN 1..query_count LOOP
        IF queries[i] = 0 AND NOT run_all THEN
            continue;
        END IF;

        IF run_all THEN
            SELECT ret_id, new_duration, old_duration, ret_checked INTO run_record FROM tpch_run_internal(i, true);
        ELSE 
            SELECT ret_id, new_duration, old_duration, ret_checked INTO run_record FROM tpch_run_internal(queries[i]);
        END IF;

        "Qid" := to_char(run_record.ret_id, '09');
        "Current(ms)" := to_char(run_record.new_duration, '9999990.99');
        "Stable(ms)" := to_char(run_record.old_duration, '9999990.99');
        "Diff(%)" := to_char((run_record.new_duration - run_record.old_duration) / run_record.old_duration * 100, 's990.99');
        "Result" := (run_record.ret_checked)::text;

        sum_run := sum_run + run_record.new_duration;
        sum_statble := sum_statble + run_record.old_duration;

        RETURN NEXT;
    END LOOP;
    "Qid" := '----';
    "Stable(ms)" := '-----------';
    "Current(ms)" := '-----------';
    "Diff(%)" := '-------';
    "Result" := '';
    RETURN NEXT;

    "Qid" := 'Sum';
    "Stable(ms)" := to_char(sum_statble, '9999990.99');
    "Current(ms)" := to_char(sum_run, '9999990.99');
    "Diff(%)" := to_char((sum_run - sum_statble) / sum_statble * 100, 's990.99');
    "Result" := '';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE VIEW tpch AS
SELECT
  *
FROM
  tpch();

SELECT
  tpch_prepare();