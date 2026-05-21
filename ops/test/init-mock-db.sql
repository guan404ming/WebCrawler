-- Mock crawlerdb schema for docker-compose.test.yml.
--
-- Loaded automatically by postgres:16 via
-- /docker-entrypoint-initdb.d/01-init-schema.sql on first container start.
--
-- Mirrors the prod schema after all in-tree migrations have been applied
-- (see docs/04-sql-schema-design.md + scripts/migrate_*.py). The ONE thing
-- it does NOT create is `domain_sitemap` — that table is created later by
-- scripts/migrate_add_domain_sitemap.py, which is precisely the migration
-- this test stack exercises.

-- ---------------------------------------------------------------------------
-- Non-sharded tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS domain_state (
    domain_id           BIGSERIAL PRIMARY KEY,
    domain              VARCHAR NOT NULL UNIQUE,
    shard_id            INTEGER NOT NULL,
    domain_score        FLOAT DEFAULT 0.0,
    crawl_paused_until  TIMESTAMPTZ,
    domain_fail_count   INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_domain_state_shard_score
    ON domain_state (shard_id, domain_score DESC NULLS LAST, domain_id);

CREATE TABLE IF NOT EXISTS shard_split_subdomain (
    host         VARCHAR PRIMARY KEY,
    migrated_at  TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Sharded URL tables: 256 each of url_state_current / url_state_history /
-- url_event_counter. Created in a plpgsql DO loop so the file stays short.
-- ---------------------------------------------------------------------------

DO $bootstrap$
DECLARE
    i INT;
    shard TEXT;
BEGIN
    FOR i IN 0..255 LOOP
        shard := lpad(i::text, 3, '0');

        -- url_state_current_NNN
        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS url_state_current_%s (
                url                     TEXT PRIMARY KEY,
                domain_id               BIGINT NOT NULL,
                first_seen              TIMESTAMPTZ DEFAULT now(),
                last_scheduled          TIMESTAMPTZ,
                last_fetch_ok           TIMESTAMPTZ,
                last_content_update     TIMESTAMPTZ,
                num_scheduled_90d       INTEGER DEFAULT 0,
                num_fetch_ok_90d        INTEGER DEFAULT 0,
                num_fetch_fail_90d      INTEGER DEFAULT 0,
                num_content_update_90d  INTEGER DEFAULT 0,
                num_consecutive_fail    INTEGER DEFAULT 0,
                last_fail_reason        TEXT,
                content_hash            TEXT,
                should_crawl            BOOLEAN DEFAULT TRUE,
                url_score               FLOAT DEFAULT 0.0,
                url_score_updated_at    TIMESTAMPTZ,
                domain_score            FLOAT DEFAULT 0.0,
                source                  SMALLINT NOT NULL DEFAULT 0,
                discovered_from         VARCHAR,
                title                   VARCHAR,
                hreflang_count          INTEGER,
                has_json_ld             BOOLEAN,
                last_modified           TIMESTAMPTZ,
                etag                    VARCHAR,
                cache_control           VARCHAR,
                is_redirect             BOOLEAN,
                redirect_hop_count      SMALLINT,
                discovery_source_type   SMALLINT NOT NULL DEFAULT 0,
                parent_page_score       DOUBLE PRECISION,
                inlink_count_approx     INTEGER NOT NULL DEFAULT 0,
                inlink_count_external   INTEGER NOT NULL DEFAULT 0,
                anchor_text             VARCHAR,
                robots_bits             SMALLINT NOT NULL DEFAULT 0
            )
        $f$, shard);

        -- url_state_history_NNN  (append-only snapshots; same columns + snapshot_id)
        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS url_state_history_%s (
                snapshot_id             BIGSERIAL PRIMARY KEY,
                snapshot_at             TIMESTAMPTZ DEFAULT now(),
                url                     VARCHAR NOT NULL,
                domain_id               BIGINT NOT NULL,
                first_seen              TIMESTAMPTZ,
                last_scheduled          TIMESTAMPTZ,
                last_fetch_ok           TIMESTAMPTZ,
                last_content_update     TIMESTAMPTZ,
                num_scheduled_90d       INTEGER DEFAULT 0,
                num_fetch_ok_90d        INTEGER DEFAULT 0,
                num_fetch_fail_90d      INTEGER DEFAULT 0,
                num_content_update_90d  INTEGER DEFAULT 0,
                num_consecutive_fail    INTEGER DEFAULT 0,
                last_fail_reason        TEXT,
                content_hash            TEXT,
                should_crawl            BOOLEAN DEFAULT TRUE,
                url_score               FLOAT DEFAULT 0.0,
                url_score_updated_at    TIMESTAMPTZ,
                domain_score            FLOAT DEFAULT 0.0,
                source                  SMALLINT NOT NULL DEFAULT 0,
                discovered_from         VARCHAR,
                title                   VARCHAR,
                hreflang_count          INTEGER,
                has_json_ld             BOOLEAN,
                last_modified           TIMESTAMPTZ,
                etag                    VARCHAR,
                cache_control           VARCHAR,
                is_redirect             BOOLEAN,
                redirect_hop_count      SMALLINT,
                discovery_source_type   SMALLINT NOT NULL DEFAULT 0,
                parent_page_score       DOUBLE PRECISION,
                inlink_count_approx     INTEGER NOT NULL DEFAULT 0,
                inlink_count_external   INTEGER NOT NULL DEFAULT 0,
                anchor_text             VARCHAR,
                robots_bits             SMALLINT NOT NULL DEFAULT 0
            )
        $f$, shard);

        -- url_event_counter_NNN (ingestor reads/writes for crawl results;
        -- "new" link records do not touch this table, but the ingestor
        -- module still imports the table name on init for some paths.)
        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS url_event_counter_%s (
                url                  VARCHAR NOT NULL,
                event_date           DATE NOT NULL,
                num_scheduled        INTEGER DEFAULT 0,
                num_fetch_ok         INTEGER DEFAULT 0,
                num_fetch_fail       INTEGER DEFAULT 0,
                num_content_update   INTEGER DEFAULT 0,
                accounted            BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (url, event_date)
            )
        $f$, shard);
    END LOOP;
END
$bootstrap$;
