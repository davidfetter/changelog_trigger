CREATE TABLE IF NOT EXISTS the_log (
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    "user" text NOT NULL DEFAULT CURRENT_USER,
    action text NOT NULL,
    table_schema text NOT NULL,
    table_name text NOT NULL,
    old_row jsonb,
    new_row jsonb,
    CONSTRAINT the_log_check CHECK ( CASE action WHEN 'INSERT' THEN old_row IS NULL WHEN 'DELETE' THEN new_row IS NULL END )
) PARTITION BY LIST(table_schema);

CREATE OR REPLACE FUNCTION log()
RETURNS TRIGGER
SECURITY DEFINER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'This function should not fire on %', TG_OP;
    END IF;

    CASE TG_OP 
    WHEN 'INSERT' THEN
        INSERT INTO the_log (
            action,            table_schema,    table_name, new_row
        )
        SELECT
            TG_OP, TG_TABLE_SCHEMA, TG_RELNAME, row_to_json(new_table)::jsonb
        FROM
            new_table;
    WHEN 'DELETE' THEN
        INSERT INTO the_log (
            action,            table_schema,    table_name, old_row
        )
        SELECT
            TG_OP, TG_TABLE_SCHEMA, TG_RELNAME, row_to_json(old_table)::jsonb
        FROM
            old_table;
    ELSE /* UPDATE */
        /*
         *  DANGER, WILL ROBINSON!  DANGER!
         *  This implementation assumes based on current implementation details
         *  that old_table and new_table have identical orderings.  Should that
         *  implementation detail change, this will get a lot more complicated.
         */

        INSERT INTO the_log (
            action, table_schema,    table_name, old_row, new_row
        )
        SELECT
            TG_OP,  TG_TABLE_SCHEMA, TG_RELNAME, old_row, new_row
        FROM
            UNNEST(
                ARRAY(SELECT row_to_json(old_table)::jsonb FROM old_table),
                ARRAY(SELECT row_to_json(new_table)::jsonb FROM new_table)
            ) AS t(old_row, new_row)
    END CASE;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION add_logging_items(schema_name TEXT, table_name TEXT)
RETURNS VOID
SECURITY DEFINER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Adding log table(s) for %.%', schema_name, table_name;

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I
    PARTITION OF the_log
    FOR VALUES IN (%L)
        PARTITION BY LIST(table_name)',
        pg_catalog.concat_ws('_', schema_name, 'log'),
        schema_name
    );

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I
    PARTITION OF %s
    FOR VALUES IN (%L)',
        pg_catalog.concat_ws('_', schema_name, table_name, 'log'),
        pg_catalog.concat_ws('_', schema_name, 'log'),
        table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER INSERT ON %I.%I
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log()$q$,
            pg_catalog.concat_ws('_', 'log_insert', schema_name, table_name),
            schema_name,
            table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER UPDATE ON %I.%I
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log()$q$,
            pg_catalog.concat_ws('_', 'log_update', schema_name, table_name),
            schema_name,
            table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER DELETE ON %I.%I
    REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log()$q$,
            pg_catalog.concat_ws('_', 'log_delete', schema_name, table_name),
            schema_name,
            table_name
    );
RETURN;
END;
$$;

COMMENT ON FUNCTION add_logging_items(schema_name TEXT, table_name TEXT) IS $$This is a stand-alone function in case we need to back-fill$$;

CREATE OR REPLACE FUNCTION add_logger()
RETURNS event_trigger
SECURITY DEFINER
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN

    SELECT p.*, c.relname as table_name INTO STRICT r
    FROM
        pg_catalog.pg_event_trigger_ddl_commands() p
    JOIN
        pg_catalog.pg_class c
        ON (p.objid = c.oid)
    WHERE
        p.object_type = 'table' AND
        c.relname !~ '_log$'; /* Let's not recurse here ;) */

    IF NOT FOUND THEN
        RAISE NOTICE 'Skipping log table';
        RETURN;
    END IF;

    PERFORM add_logging_items(r.schema_name, r.table_name);

    EXCEPTION
        WHEN no_data_found THEN
            NULL;
        WHEN too_many_rows THEN
            RAISE EXCEPTION 'This function should only fire on one table, not this list: %', r.object_identity;
END;
$$;

CREATE EVENT TRIGGER add_logger
    ON ddl_command_end
    WHEN tag IN ('create table')
        EXECUTE PROCEDURE add_logger();
