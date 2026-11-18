import * as PgClient from "@effect/sql-pg/PgClient"
import * as Arr from "effect/Array"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { SqlClient } from "effect/sql"
import { Grant, privateScope, type Principal } from "../domain/Scope.ts"
import * as Wire from "../protocol.ts"

/** Must run inside the serialized engine transaction. Never changes receipt/authority identity. */
export const compact = Effect.fnUntraced(function* (through: number) {
  const sql = yield* SqlClient.SqlClient
  yield* Schema.decodeEffect(Wire.Position)(through)
  yield* sql`INSERT INTO echosync_scope_floors(scope,cursor)
    SELECT scope,least(cursor,${through}) FROM echosync_scopes
    ON CONFLICT(scope) DO UPDATE SET cursor=greatest(echosync_scope_floors.cursor,EXCLUDED.cursor)`
  yield* sql`DELETE FROM echosync_scope_changes c USING echosync_scope_floors f WHERE c.scope=f.scope AND c.revision<=f.cursor`
})

/** Must run inside the serialized engine transaction. Drops each scope's change history older
 * than the retention window and raises that scope's floor only to the newest dropped revision,
 * so a device whose cursor is at or past it keeps incremental catchup. */
export const expire = Effect.fnUntraced(function* (retentionMillis: number) {
  const sql = yield* SqlClient.SqlClient
  yield* sql`INSERT INTO echosync_scope_floors(scope,cursor)
    SELECT scope,max(revision) FROM echosync_scope_changes
    WHERE at <= clock_timestamp() - ${retentionMillis} * interval '1 millisecond' GROUP BY scope
    ON CONFLICT(scope) DO UPDATE SET cursor=greatest(echosync_scope_floors.cursor,EXCLUDED.cursor)`
  yield* sql`DELETE FROM echosync_scope_changes c USING echosync_scope_floors f WHERE c.scope=f.scope AND c.revision<=f.cursor`
})

/** Entries per Pull page. Each page is a round trip plus a staged-read transaction, so a
 * catalog bootstrap's latency is dominated by page count; 8192 rows of a few hundred bytes
 * stay far below the client's 64 Mi frame limit. */
const pageSize = 8192

/** A catchup that fits one page replies directly and writes nothing. A larger one materializes a
 * fixed view before paging; no SQL transaction or connection spans requests. */
export const pull = Effect.fnUntraced(function* (principal: Principal, request: Wire.PullRequest) {
  const sql = yield* PgClient.PgClient
  const reply = yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
      const engines = yield* sql<{
        readonly epoch: string
        readonly revision: string
        readonly generation: string
      }>`SELECT epoch,cursor::text AS revision,sync_generation::text AS generation FROM echosync_engine`
      const engine = engines[0]
      if (engine === undefined) return yield* new Wire.ServerError({ message: "Run EchoSync migrate" })
      if (request.epoch !== null && request.epoch !== engine.epoch)
        return yield* new Wire.ResyncRequired({ epoch: engine.epoch })
      const auth = yield* sql<{
        readonly position: string
      }>`SELECT coalesce(max(position),0)::text AS position FROM echosync_grant_events`
      const authorization = auth[0]?.position ?? "0"
      let id = request.continuation?.id
      if (id === undefined) {
        const grants = yield* Schema.decodeUnknownEffect(Schema.Array(Grant))(
          yield* sql`SELECT scope,principal,role FROM echosync_grants WHERE principal=${principal}`
        )
        const scopes = yield* sql<{ readonly scope: string; readonly cursor: string; readonly floor: string }>`
        SELECT s.scope,s.cursor::text,greatest(coalesce(f.cursor,0),coalesce(gf.cursor,0))::text AS floor FROM
        (SELECT scope,cursor FROM echosync_scopes UNION SELECT ${privateScope(principal)},0 WHERE NOT EXISTS(SELECT 1 FROM echosync_scopes WHERE scope=${privateScope(principal)})) s
        LEFT JOIN echosync_scope_floors f ON f.scope=s.scope
        LEFT JOIN echosync_grant_floors gf ON gf.scope=s.scope AND gf.principal=${principal}
        WHERE s.scope=${privateScope(principal)} OR s.scope LIKE 'catalog:%' OR EXISTS(SELECT 1 FROM echosync_grants g WHERE g.scope=s.scope AND g.principal=${principal}) ORDER BY s.scope`
        const response: Wire.PullResponse = {
          epoch: engine.epoch,
          revision: Number(engine.revision),
          grants,
          scopes: Arr.map(scopes, ({ scope, cursor, floor }) => ({
            scope,
            cursor: Number(cursor),
            reset:
              request.cursors[scope] === undefined ||
              (request.cursors[scope] ?? 0) < Number(floor) ||
              (request.cursors[scope] ?? 0) > Number(cursor),
            entries: []
          }))
        }
        // Ranked entries of one scope, in the order staging and direct replies both use.
        const source = (scope: (typeof response.scopes)[number]) =>
          scope.reset
            ? sql`SELECT row_number() OVER(ORDER BY aggregate,id) AS rank,
                jsonb_build_object('_tag','Upsert','aggregate',aggregate,'id',id,'seq',seq,'state',state) AS entry
                FROM echosync_streams WHERE scope=${scope.scope}`
            : sql`SELECT row_number() OVER(ORDER BY keys.aggregate,keys.id) AS rank,
            CASE WHEN s.id IS NULL THEN jsonb_build_object('_tag','Gone','aggregate',keys.aggregate,'id',keys.id)
            ELSE jsonb_build_object('_tag','Upsert','aggregate',s.aggregate,'id',s.id,'seq',s.seq,'state',s.state) END AS entry
          FROM (
            SELECT DISTINCT aggregate,id FROM echosync_scope_changes WHERE scope=${scope.scope} AND revision>${request.cursors[scope.scope] ?? 0} AND revision<=${scope.cursor}
          ) keys LEFT JOIN echosync_streams s ON s.aggregate=keys.aggregate AND s.id=keys.id AND s.scope=${scope.scope}`
        // Most catchups fit one page: answer from this snapshot and write nothing. Staging only
        // exists so later pages share one view, so a single-page reply needs none.
        const direct = new Map<string, Array<typeof Wire.Entry.Type>>()
        let remaining = pageSize
        for (const scope of response.scopes) {
          const rows = yield* sql<{
            readonly entry: unknown
          }>`SELECT entry FROM (${source(scope)}) ranked ORDER BY rank LIMIT ${remaining + 1}`
          remaining -= rows.length
          if (remaining < 0) break
          const values: Array<typeof Wire.Entry.Type> = []
          for (const row of rows) values.push(yield* Schema.decodeUnknownEffect(Wire.Entry)(row.entry))
          direct.set(scope.scope, values)
        }
        if (remaining >= 0)
          return {
            ...response,
            scopes: Arr.map(response.scopes, (scope) => ({ ...scope, entries: direct.get(scope.scope) ?? [] }))
          }
        const sessions = yield* sql<{
          readonly id: string
        }>`INSERT INTO echosync_pull_sessions(principal,epoch,grant_position,generation,response) VALUES(${principal},${engine.epoch},${authorization},${engine.generation},${sql.json(response)}) RETURNING id::text`
        id = sessions[0]?.id
        if (id === undefined) return yield* new Wire.ServerError({ message: "Cannot stage Pull" })
        let offset = 0
        for (const scope of response.scopes) {
          const inserted = yield* sql<{ readonly count: string }>`WITH inserted AS (
            INSERT INTO echosync_pull_entries(session,position,scope,entry)
            SELECT ${id}::uuid,${offset}+rank,${scope.scope},entry FROM (${source(scope)}) ranked RETURNING 1
          ) SELECT count(*)::text AS count FROM inserted`
          offset += Number(inserted[0]?.count ?? 0)
        }
      }
      const sessions = yield* sql<{
        readonly response: unknown
      }>`SELECT response FROM echosync_pull_sessions WHERE id=${id}::uuid AND principal=${principal} AND epoch=${engine.epoch} AND grant_position=${authorization} AND generation=${engine.generation} AND expires_at>now()`
      const session = sessions[0]
      // Expiry/revocation is retryable without resetting the durable replica or journal.
      if (session === undefined)
        return yield* new Wire.ServerError({ message: "Pull continuation expired; restart catchup" })
      const response = yield* Schema.decodeUnknownEffect(Wire.PullResponse)(session.response)
      const offset = request.continuation?.offset ?? 0
      const rows = yield* sql<{
        readonly position: string
        readonly scope: string
        readonly entry: unknown
      }>`SELECT position::text,scope,entry FROM echosync_pull_entries WHERE session=${id}::uuid AND position>${offset} ORDER BY echosync_pull_entries.position LIMIT ${pageSize + 1}`
      const page = rows.slice(0, pageSize)
      const entries = new Map<string, Array<typeof Wire.Entry.Type>>()
      for (const row of page) {
        const values = entries.get(row.scope) ?? []
        values.push(yield* Schema.decodeUnknownEffect(Wire.Entry)(row.entry))
        entries.set(row.scope, values)
      }
      return {
        ...response,
        scopes: Arr.map(response.scopes, (scope) => ({ ...scope, entries: entries.get(scope.scope) ?? [] })),
        page: { id, next: rows.length > pageSize ? Number(page[page.length - 1]?.position ?? offset) : null }
      }
    })
  )
  // Only stagers create sessions, so they clean up expired ones. In autocommit, outside the
  // REPEATABLE READ snapshot: concurrent stagers then wait and delete nothing instead of failing
  // with a serialization error.
  if (request.continuation === undefined && reply.page !== undefined)
    yield* sql`DELETE FROM echosync_pull_sessions WHERE expires_at<=now()`
  return reply
})
