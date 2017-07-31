= Changelog Triggers =

Knowing what happened at a logical level and, at least approximately, when, is
something people often want...frequently when it's late because making those
things available is tedious and error-prone and so gets skipped at CREATE TABLE
time, when it's really needed.

This project helps make having those things around simple and light-weight
operation.

== Logging Table ==

To safeguard against rowtype changes, the logging table blobs the old and new
versions of a row each into a json blob.  This way, no maintenance needs to
happen on ALTER TABLE.

== Event Trigger ==

On CREATE TABLE, this trigger creates a log partition and one (or more, for
PostgreSQL >= 10) logging triggers which automatically log changes on the table
to the logging table created.

== Logging Trigger(s) ==

For PostgreSQL < 10, one trigger gets created.  It fires after each row, and
logs the appropriate change to the table's log table.

For PostgreSQL >= 10, three triggers need to be created, firing after each
*statement*.  There need to be three because they refer to transition tables,
different combinations of which actually exist on INSERT, UPDATE, and DELETE.
