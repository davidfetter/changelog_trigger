DO LANGUAGE plpgsql $plpgsql$
DECLARE
    modern BOOLEAN;
BEGIN

    SELECT setting::integer >= 100000 INTO modern
    FROM pg_catalog.pg_settings
    WHERE "name" = 'server_version_num';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'server_version_num setting not found in pg_catalog.pg_settings.  Bailing out!';
    END IF;

    EXECUTE format($q$CREATE TABLE IF NOT EXISTS the_log (
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    "user" text NOT NULL DEFAULT CURRENT_USER,
    action text NOT NULL,
    table_schema text NOT NULL,
    table_name text NOT NULL,
    old_row jsonb,
    new_row jsonb,
    CONSTRAINT the_log_check CHECK ( CASE action WHEN 'INSERT' THEN old_row IS NULL WHEN 'DELETE' THEN new_row IS NULL END )
)%s$q$, CASE WHEN modern THEN ' PARTITION BY LIST(table_schema)' ELSE '' END);

    IF modern THEN
CREATE OR REPLACE FUNCTION log()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'This function should not fire on %', TG_OP;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO the_log (
            action,            table_schema,    table_name, new_row
        )
        SELECT
            TG_OP, TG_TABLE_SCHEMA, TG_RELNAME, row_to_json(new_table)::jsonb
        FROM
            new_table;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO the_log (
            action,            table_schema,    table_name, old_row
        )
        SELECT
            TG_OP, TG_TABLE_SCHEMA, TG_RELNAME, row_to_json(old_table)::jsonb
        FROM
            old_table;
    ELSE
        /*
         *  DANGER, WILL ROBINSON!  DANGER!
         *  This implementation assumes based on current implementation details
         *  that old_table and new_table have identical orderings.  Should that
         *  implementation detail change, this will get a lot more complicated.
         */

        WITH
            o AS (SELECT row_to_json(old_table)::jsonb AS old_row, row_number() OVER () AS ord FROM old_table),
            n AS (SELECT row_to_json(new_table)::jsonb AS new_row, row_number() OVER () AS ord FROM new_table)
        INSERT INTO the_log (
            action, table_schema,    table_name, old_row, new_row
        )
        SELECT
            TG_OP,  TG_TABLE_SCHEMA, TG_RELNAME, old_row, new_row
        FROM
            o
        JOIN
            n
            USING(ord);
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION add_logger()
RETURNS event_trigger
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

    RAISE NOTICE 'Adding log table(s) for %.%', r.schema_name, r.table_name;

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I
    PARTITION OF the_log
    FOR VALUES IN (%L)
        PARTITION BY LIST(table_name);',
        pg_catalog.concat_ws('_', r.schema_name, 'log'),
        r.schema_name
    );

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I
    PARTITION OF %s
    FOR VALUES IN (%L);',
        pg_catalog.concat_ws('_', r.schema_name, r.table_name, 'log'),
        pg_catalog.concat_ws('_', r.schema_name, 'log'),
        r.table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER INSERT ON %I.%I
    REFERENCING NEW TABLE AS new_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log();$q$,
            pg_catalog.concat_ws('_', 'log_insert', r.schema_name, r.table_name),
            r.schema_name,
            r.table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER UPDATE ON %I.%I
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log();$q$,
            pg_catalog.concat_ws('_', 'log_update', r.schema_name, r.table_name),
            r.schema_name,
            r.table_name
    );

    EXECUTE format(
            $q$CREATE TRIGGER %I
    AFTER DELETE ON %I.%I
    REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT
        EXECUTE PROCEDURE public.log();$q$,
            pg_catalog.concat_ws('_', 'log_delete', r.schema_name, r.table_name),
            r.schema_name,
            r.table_name
    );

    EXCEPTION
        WHEN no_data_found THEN
            NULL;
        WHEN too_many_rows THEN
            RAISE EXCEPTION 'This function should only fire on one table, not this list: %', r.object_identity;
END;
$$;
    ELSE
CREATE OR REPLACE FUNCTION add_logger()
RETURNS event_trigger
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

    RAISE NOTICE 'Adding log table(s) for %.%', r.schema_name, r.table_name;

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I(
    CHECK(table_schema = %L)
) INHERITS(the_log);',
        pg_catalog.concat_ws('_', r.schema_name, 'log'),
        r.schema_name
    );

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I(
    CHECK (table_name = %L)
) INHERITS(%s)',
        pg_catalog.concat_ws('_', r.schema_name, r.table_name, 'log'),
        r.table_name,
        pg_catalog.concat_ws('_', r.schema_name, 'log')
    );

    EXECUTE format(
            $q$CREATE OR REPLACE FUNCTION %I()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $trig$
BEGIN
    INSERT INTO %I (
        action,
        table_schema,
        table_name,
        old_row,
        new_row
    )
    VALUES (
        TG_OP,
        TG_TABLE_SCHEMA,
        TG_RELNAME,
        CASE WHEN TG_OP <> 'INSERT' THEN row_to_json(OLD)::jsonb END,
        CASE WHEN TG_OP <> 'DELETE' THEN row_to_json(NEW)::jsonb END
    );
    RETURN NULL;
END;
$trig$;

CREATE TRIGGER %I
    AFTER INSERT OR UPDATE OR DELETE ON %I.%I
    FOR EACH ROW
        EXECUTE PROCEDURE %I();$q$,
            pg_catalog.concat_ws('_', 'log', r.schema_name, r.table_name),
            pg_catalog.concat_ws('_', r.schema_name, r.table_name, 'log'),
            pg_catalog.concat_ws('_', 'log', r.schema_name, r.table_name),
            r.schema_name,
            r.table_name,
            pg_catalog.concat_ws('_', 'log', r.schema_name, r.table_name)
    );

    EXCEPTION
        WHEN no_data_found THEN
            NULL;
        WHEN too_many_rows THEN
            RAISE EXCEPTION 'This function should only fire on one table, not this list: %', r.object_identity;
END;
$$;
    END IF;

CREATE EVENT TRIGGER add_logger
    ON ddl_command_end
    WHEN tag IN ('create table')
        EXECUTE PROCEDURE add_logger();

END;
$plpgsql$;
