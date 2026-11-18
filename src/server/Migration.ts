import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { SqlClient } from "effect/sql"
import * as Wire from "../protocol.ts"

/**
 * The engine schema as an ordered list of released steps; the schema version is the number of
 * steps, recorded with each step's definition in `echosync_schema`. Change engine DDL only by
 * appending a step: a released step that is edited, removed or reordered makes every database
 * that applied it report `diverged` and refuse service and migration.
 *
 * Steps must stay additive, because the previous release keeps serving during a rolling deploy
 * and after a rollback: add tables, nullable or defaulted columns and indexes; never drop,
 * rename or narrow what a released Server reads or writes, including the positional INSERTs.
 */
const steps: ReadonlyArray<ReadonlyArray<string>> = [
  [
    // One-time in-place naming migration. These source names are not runtime aliases.
    `DO $echosync$
    DECLARE
      suffix text;
      old_name text;
      new_name text;
      table_id oid;
      item record;
    BEGIN
      FOREACH suffix IN ARRAY ARRAY['engine','scopes','streams','events','receipts','catalog_releases','grants','grant_events','imports','connections','presence'] LOOP
        old_name := 'echo_' || suffix;
        new_name := 'echosync_' || suffix;
        IF to_regclass(format('%I.%I',current_schema(),old_name)) IS NOT NULL THEN
          IF to_regclass(format('%I.%I',current_schema(),new_name)) IS NOT NULL THEN
            RAISE EXCEPTION 'EchoSync naming collision for %', new_name;
          END IF;
          EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',current_schema(),old_name,new_name);
        END IF;
        table_id := to_regclass(format('%I.%I',current_schema(),new_name));
        FOR item IN SELECT conname FROM pg_constraint WHERE conrelid=table_id AND starts_with(conname,'echo_') LOOP
          EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',current_schema(),new_name,item.conname,'echosync_' || substr(item.conname,6));
        END LOOP;
        FOR item IN SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=table_id AND starts_with(c.relname,'echo_') LOOP
          EXECUTE format('ALTER INDEX %I.%I RENAME TO %I',current_schema(),item.relname,'echosync_' || substr(item.relname,6));
        END LOOP;
      END LOOP;
      IF to_regclass(format('%I.echo_grant_events_position_seq',current_schema())) IS NOT NULL THEN
        EXECUTE format('ALTER SEQUENCE %I.echo_grant_events_position_seq RENAME TO echosync_grant_events_position_seq',current_schema());
      END IF;
    END
  $echosync$`,
    `CREATE TABLE IF NOT EXISTS echosync_engine (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), epoch text NOT NULL, cursor bigint NOT NULL DEFAULT 0)`,
    `INSERT INTO echosync_engine(singleton,epoch) VALUES(true,gen_random_uuid()::text) ON CONFLICT DO NOTHING`,
    `CREATE TABLE IF NOT EXISTS echosync_scopes (scope text PRIMARY KEY,cursor bigint NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS echosync_streams (aggregate text NOT NULL,id text NOT NULL,scope text NOT NULL,seq integer NOT NULL,state jsonb NOT NULL,PRIMARY KEY(aggregate,id))`,
    `CREATE TABLE IF NOT EXISTS echosync_events (aggregate text NOT NULL,id text NOT NULL,seq integer NOT NULL,payload jsonb NOT NULL,batch_id text NOT NULL,principal text NOT NULL,at timestamptz NOT NULL,PRIMARY KEY(aggregate,id,seq))`,
    `CREATE TABLE IF NOT EXISTS echosync_receipts (principal text NOT NULL,client_id text NOT NULL,batch_id text NOT NULL,request jsonb NOT NULL,receipt jsonb NOT NULL,PRIMARY KEY(principal,client_id,batch_id))`,
    `CREATE TABLE IF NOT EXISTS echosync_catalog_releases (scope text NOT NULL,release text NOT NULL,cursor bigint NOT NULL,PRIMARY KEY(scope,release))`,
    `CREATE TABLE IF NOT EXISTS echosync_grants (scope text NOT NULL,principal text NOT NULL,role text NOT NULL CHECK(role IN ('reader','writer','owner')),PRIMARY KEY(scope,principal))`,
    `CREATE TABLE IF NOT EXISTS echosync_grant_events (position bigserial PRIMARY KEY,scope text NOT NULL,subject text NOT NULL,role text,actor text NOT NULL,batch_id text NOT NULL,at timestamptz NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS echosync_imports (id text PRIMARY KEY,request jsonb NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS echosync_connections (id text PRIMARY KEY,principal text NOT NULL,expires_at timestamptz NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS echosync_presence (connection_id text NOT NULL REFERENCES echosync_connections(id) ON DELETE CASCADE,room text NOT NULL,client_id text NOT NULL,state jsonb NOT NULL,expires_at timestamptz NOT NULL,PRIMARY KEY(connection_id,room,client_id))`,
    // After the engine lock, like every writer: locking streams first could deadlock a live writer.
    `UPDATE echosync_engine SET cursor=cursor`,
    // Pattern ops serve both scope equality and the collation-independent 'catalog:%' prefix.
    `CREATE INDEX IF NOT EXISTS echosync_streams_scope ON echosync_streams(scope text_pattern_ops,aggregate,id)`,
    `ALTER TABLE echosync_engine ADD COLUMN IF NOT EXISTS sync_generation bigint NOT NULL DEFAULT 0`,
    // Existing cursors become the log floor when change tracking first starts. Only then: a
    // scope created later has complete history, and re-raising its floor would force its
    // devices to bootstrap.
    `DO $echosync$
    BEGIN
      IF to_regclass(format('%I.echosync_scope_floors',current_schema())) IS NULL THEN
        CREATE TABLE echosync_scope_floors(scope text PRIMARY KEY, cursor bigint NOT NULL);
        INSERT INTO echosync_scope_floors SELECT scope,cursor FROM echosync_scopes;
      END IF;
    END
  $echosync$`,
    // A principal's own catchup floor per granted scope: the revision its grant took effect.
    // Grants older than this table are covered by the scope floors their grant raised.
    `CREATE TABLE IF NOT EXISTS echosync_grant_floors(scope text NOT NULL, principal text NOT NULL, cursor bigint NOT NULL, PRIMARY KEY(scope,principal))`,
    `CREATE TABLE IF NOT EXISTS echosync_scope_changes(scope text NOT NULL, revision bigint NOT NULL, aggregate text NOT NULL,id text NOT NULL,PRIMARY KEY(scope,revision,aggregate,id))`,
    // Engine writers hold the singleton lock and advance it once after all stream mutations.
    // Store identities, not historical state: catchup returns only the authorized current view.
    // Age for retention only chooses each scope's floor; deletion is by revision at or below it,
    // so imperfect ordering (rows that predate the column) keeps history longer, never loses it.
    `ALTER TABLE echosync_scope_changes ADD COLUMN IF NOT EXISTS at timestamptz NOT NULL DEFAULT clock_timestamp()`,
    `CREATE OR REPLACE FUNCTION echosync_track_stream() RETURNS trigger LANGUAGE plpgsql AS $body$
    DECLARE revision bigint;
    BEGIN
      SELECT cursor+1 INTO revision FROM echosync_engine;
      IF TG_OP <> 'INSERT' THEN
        INSERT INTO echosync_scope_changes VALUES(OLD.scope,revision,OLD.aggregate,OLD.id) ON CONFLICT DO NOTHING;
      END IF;
      IF TG_OP <> 'DELETE' THEN
        INSERT INTO echosync_scope_changes VALUES(NEW.scope,revision,NEW.aggregate,NEW.id) ON CONFLICT DO NOTHING;
      END IF;
      RETURN NULL;
    END
  $body$`,
    `DROP TRIGGER IF EXISTS echosync_track_stream ON echosync_streams`,
    `CREATE TRIGGER echosync_track_stream AFTER INSERT OR UPDATE OR DELETE ON echosync_streams FOR EACH ROW EXECUTE FUNCTION echosync_track_stream()`,
    `CREATE TABLE IF NOT EXISTS echosync_pull_sessions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),principal text NOT NULL,epoch text NOT NULL,grant_position bigint NOT NULL,generation bigint NOT NULL,expires_at timestamptz NOT NULL DEFAULT now()+interval '5 minutes',response jsonb NOT NULL)`,
    `CREATE INDEX IF NOT EXISTS echosync_pull_expiry ON echosync_pull_sessions(expires_at)`,
    `CREATE TABLE IF NOT EXISTS echosync_pull_entries(session uuid NOT NULL REFERENCES echosync_pull_sessions(id) ON DELETE CASCADE,position bigint NOT NULL,scope text NOT NULL,entry jsonb NOT NULL,PRIMARY KEY(session,position))`
  ]
]
const definitions = Arr.map(steps, Arr.join(";\n"))

/** The engine schema is behind this code (`pending`) or records a changed released step
 * (`diverged`); run `Server.migrate`. Server API only: over HTTP it is a `ServerError`, because
 * clients already open decode only the existing error variants. */
export class SchemaIncompatible extends Schema.TaggedError<SchemaIncompatible, { readonly brand: unique symbol }>()(
  "SchemaIncompatible",
  { status: Schema.Literals(["pending", "diverged"]), recorded: Wire.Position, expected: Wire.Position }
) {}

export interface SchemaStatus {
  /** `pending`: the database lacks steps this code needs, including a database never migrated
   * (recorded 0). `ahead`: it has steps this code does not know, as after a rollback; served.
   * `diverged`: a step this code knows was recorded with a different definition. */
  readonly status: "current" | "pending" | "ahead" | "diverged"
  readonly recorded: number
  readonly expected: number
}

/** Needs only the SQL client, so an application can check before constructing a Server. */
export const schemaStatus = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const expected = definitions.length
  const table = yield* sql<{
    readonly found: boolean
  }>`SELECT to_regclass(format('%I.echosync_schema',current_schema())) IS NOT NULL AS found`
  const rows =
    table[0]?.found === true
      ? yield* sql<{
          readonly step: number
          readonly definition: string
        }>`SELECT step,definition FROM echosync_schema ORDER BY step`
      : []
  const recorded = rows[rows.length - 1]?.step ?? 0
  const known = Math.min(recorded, expected)
  const status: SchemaStatus["status"] = Arr.some(
    rows.slice(0, known),
    (row, i) => row.step !== i + 1 || row.definition !== definitions[i]
  )
    ? "diverged"
    : recorded < expected
      ? "pending"
      : recorded > expected
        ? "ahead"
        : "current"
  return { status, recorded, expected } satisfies SchemaStatus
})

/**
 * Safe to run on every startup, from several instances at once. A current or ahead database
 * costs one read: no DDL and no engine or table lock, and an older release never re-runs its
 * DDL over a newer schema. Otherwise it applies the missing steps and records them in one
 * transaction, which composes with an application's enclosing migration transaction.
 */
export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const before = yield* schemaStatus
  if (before.status === "current" || before.status === "ahead") return
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`SELECT pg_advisory_xact_lock(hashtextextended('echosync:migrate:' || current_schema(),0))`
      const status = yield* schemaStatus
      if (status.status === "current" || status.status === "ahead") return
      if (status.status === "diverged") return yield* new SchemaIncompatible({ ...status, status: status.status })
      // A versioned engine takes the engine lock before any later step's DDL, like every writer.
      if (status.recorded > 0) yield* sql`UPDATE echosync_engine SET cursor=cursor`
      yield* sql`CREATE TABLE IF NOT EXISTS echosync_schema(step integer PRIMARY KEY, definition text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`
      for (const [i, statements] of steps.slice(status.recorded).entries()) {
        for (const statement of statements) yield* sql.unsafe(statement)
        yield* sql`INSERT INTO echosync_schema(step,definition) VALUES(${status.recorded + i + 1},${Arr.join(statements, ";\n")})`
      }
    })
  )
})
