use chrono::Utc;
use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use crate::PARTITION_KEY;

#[derive(Debug, FromRow)]
struct MigrationData {
    id: String,
    status: String,
}

pub(crate) async fn session(db_url: &str) -> anyhow::Result<Session> {
    let session = SessionBuilder::new()
        .known_node(db_url)
        .build()
        .await?;

    session
        .query_unpaged(
            "
            CREATE KEYSPACE IF NOT EXISTS scylladb_migrate_ks
            WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}
            ",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "
            CREATE TABLE IF NOT EXISTS scylladb_migrate_ks.migrations
            (
                type TEXT,
                id TEXT,
                status TEXT,
                run_at TIMESTAMP,

                PRIMARY KEY (type, id)
            )
            ",
            &[],
        )
        .await?;

    Ok(session)
}

pub(crate) async fn upsert(
    session: &Session,
    migration: String,
    success: bool,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    let status = if success { "success" } else { "failed" };

    session
        .query_unpaged(
            "
                INSERT INTO scylladb_migrate_ks.migrations (type, id, status, run_at)
                VALUES (?, ?, ?, ?)
                ",
            (PARTITION_KEY, migration, status, now),
        )
        .await?;

    Ok(())
}

pub(crate) async fn list(session: &Session) -> anyhow::Result<Vec<String>> {
    Ok(
        session
            .query_unpaged(
                "
            SELECT id, status
            FROM scylladb_migrate_ks.migrations
            WHERE type = ?
            ORDER BY id
            ",
                (PARTITION_KEY,),
            )
            .await?
            .rows
            .unwrap()
            .into_typed::<MigrationData>()
            .filter_map(|r| {
                let r = r.ok()?;
                if r.status == "success" {
                    return Some(r.id);
                }
                None
            })
            .collect()
    )
}

pub(crate) async fn delete(session: &Session, migration: String) -> anyhow::Result<()> {
    session
        .query_unpaged(
            "
                DELETE FROM scylladb_migrate_ks.migrations
                WHERE type = ?
                AND id = ?
            ",
            (PARTITION_KEY, migration)
        )
        .await?;

    Ok(())
}
