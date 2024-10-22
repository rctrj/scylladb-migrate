use anyhow::Result;
use chrono::Utc;
use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use std::env::args;
use std::fs::{create_dir, read_dir, read_to_string, File};
use std::path::Path;

const ARG_KEY_PATH: &str = "-p";
const ARG_KEY_DB_URL: &str = "-u";
const ENV_KEY_PATH: &str = "SCYLLADB_MIGRATE_DIR_PATH";
const ENV_KEY_DB_URL: &str = "SCYLLADB_MIGRATE_DB_URL";

const PARTITION_KEY: &str = "migrate";

#[derive(Debug, FromRow)]
struct MigrationData {
    id: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = args().collect();
    if args.len() < 2 {
        _ = help();
        return Err(anyhow::anyhow!("Insufficient number of parameters"));
    }

    let db_url = arg_or_env(&args, ARG_KEY_DB_URL, ENV_KEY_DB_URL);
    let mut dir_path = arg_or_env(&args, ARG_KEY_PATH, ENV_KEY_PATH);
    if dir_path == "" {
        dir_path = ".".to_string()
    }
    let dir_path = dir_path;

    let command = &args[1];
    match command.as_str() {
        "generate" => generate(args, dir_path.as_str()),
        "up" => up(db_url.as_str(), dir_path.as_str()).await,
        _ => help()
    }
}

fn generate(args: Vec<String>, dir_path: &str) -> Result<()> {
    if args.len() < 3 {
        return Err(anyhow::anyhow!("Insufficient number of parameters"));
    }

    let name = args.last().unwrap(); //Should never crash as
    let date = chrono::Local::now();
    let formatted = date.format("%Y-%m-%d-%H%M%S");
    let subdirectory_path = format!("{dir_path}/{formatted}_{name}");
    let subdirectory_path = subdirectory_path.as_str();

    let up = format!("{subdirectory_path}/up.cql");
    let down = format!("{subdirectory_path}/down.cql");

    let dir = Path::new(dir_path);
    if !dir.is_dir() {
        return Err(anyhow::anyhow!("Not a directory, or does not exist: [{dir_path}]"));
    }

    create_dir(subdirectory_path)?;
    File::create(up)?;
    File::create(down)?;

    Ok(())
}

async fn up(db_url: &str, dir_path: &str) -> Result<()> {
    let session = session(db_url).await?;
    let local_migrations = subdirectories(dir_path)?;
    let db_migrations = db_migrations(&session).await?;
    println!("local migrations: {local_migrations:?}, applied migrations: {db_migrations:?}");

    let migrations_to_apply: Vec<String> = local_migrations
        .iter()
        .filter(
            |entry| !db_migrations.contains(entry)
        )
        .cloned()
        .collect();
    println!("migrations to apply: {migrations_to_apply:?}");

    // serialize is not implemented for local, so using utc
    let now = Utc::now();

    for migration in migrations_to_apply {
        let up = format!("{dir_path}/{migration}/up.cql");

        let resp = apply_migration(&session, up.as_str()).await;
        save_migration(&session, migration, resp.is_ok(), now).await?;

        if !resp.is_ok() {
            return resp
        }
    }

    Ok(())
}

async fn apply_migration(session: &Session, migration_path: &str) -> Result<()> {
    let query = file_contents(migration_path)?;
    let query = query.replace("\n", " ");

    // unable to pass queries in a single request.
    // batch request doesn't accept create table queries.
    // so splitting for now
    let queries: Vec<&str> = query
        .split(";")
        .filter(|q| !q.is_empty())
        .collect();

    println!("applying migration: {migration_path}. Query: {query}");

    for query in queries {
        session.query_unpaged(query, &[]).await?;
    }

    println!("migration applied. Successfully");
    Ok(())
}

async fn save_migration(
    session: &Session,
    migration: String,
    success: bool,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
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

async fn db_migrations(session: &Session) -> Result<Vec<String>> {
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

fn subdirectories(dir_path: &str) -> Result<Vec<String>> {
    let entries = read_dir(dir_path)?;

    let mut subdirectories: Vec<String> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if !path.is_dir() {
                return None;
            }

            let filename = path.file_name()?
                .to_str()
                .unwrap()
                .to_string();
            Some(filename)
        })
        .collect();

    subdirectories.sort();

    Ok(subdirectories)
}

fn file_contents(path: &str) -> Result<String> {
    Ok(read_to_string(path)?)
}

async fn session(db_url: &str) -> Result<Session> {
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

fn help() -> Result<()> {
    println!("Usage: abc <command> [options]
    Available commands:
        generate <name> (The last value is always supposed to be name)
        up
        down

    Available parameters:
        -p path to directory. Can also be passed using SCYLLADB_MIGRATE_DIR_PATH env var
        -u db url. Can also be passed using SCYLLADB_MIGRATE_DB_URL env var
        ");
    Ok(())
}

fn arg_or_env(args: &Vec<String>, key: &str, env_key: &str) -> String {
    if let Some(out) = arg(args, key) {
        out
    } else {
        env(env_key)
    }
}

fn arg(args: &Vec<String>, key: &str) -> Option<String> {
    let mut select_next = false;

    for arg in args {
        if select_next {
            return Some(arg.clone());
        }

        select_next = arg == key;
    }

    None
}

fn env(key: &str) -> String {
    let out = std::env::var(key);
    if let Ok(out) = out {
        out
    } else {
        String::new()
    }
}
