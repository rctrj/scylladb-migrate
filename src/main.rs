mod db;

use anyhow::Result;
use chrono::Utc;
use scylla::Session;
use std::env::args;
use std::fs::{create_dir, read_dir, read_to_string, File};
use std::path::Path;

const ARG_KEY_PATH: &str = "-p";
const ARG_KEY_DB_URL: &str = "-u";
const ARG_KEY_ALL: &str = "--all";
const ENV_KEY_PATH: &str = "SCYLLADB_MIGRATE_DIR_PATH";
const ENV_KEY_DB_URL: &str = "SCYLLADB_MIGRATE_DB_URL";

const PARTITION_KEY: &str = "migrate";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = args().collect();
    if args.len() < 2 {
        _ = help();
        return Err(anyhow::anyhow!("Insufficient number of parameters"));
    }

    let db_url = arg_or_env(&args, ARG_KEY_DB_URL, ENV_KEY_DB_URL);
    let mut dir_path = arg_or_env(&args, ARG_KEY_PATH, ENV_KEY_PATH);
    if dir_path.is_empty() {
        dir_path = ".".to_string()
    }
    let dir_path = dir_path;

    let command = &args[1];
    match command.as_str() {
        "generate" => generate(args, dir_path.as_str()),
        "up" => up(db_url.as_str(), dir_path.as_str()).await,
        "down" => down(args, db_url.as_str(), dir_path.as_str()).await,
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
    let session = db::session(db_url).await?;
    let local_migrations = subdirectories(dir_path)?;
    let db_migrations = db::list(&session).await?;
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
        db::upsert(&session, migration, resp.is_ok(), now).await?;

        resp?;
    }

    Ok(())
}

async fn down(args: Vec<String>, db_url: &str, dir_path: &str) -> Result<()> {
    let session = db::session(db_url).await?;
    let db_migrations = db::list(&session).await?;

    async fn revert(session: &Session, dir_path: &str, migrations: Vec<String>) -> Result<()> {
        let iter = migrations.iter().rev();

        for migration in iter {
            let down = format!("{dir_path}/{migration}/down.cql");
            apply_migration(session, down.as_str()).await?;
            db::delete(session, migration.clone()).await?;
        }

        Ok(())
    }

    let migrations_to_revert = if args.contains(&ARG_KEY_ALL.to_string()) {
        db_migrations
    } else if let Some(first) = db_migrations.last() {
        vec![first.clone()]
    } else {
        vec![]
    };

    if migrations_to_revert.is_empty() {
        print!("no migrations to revert");
        return Ok(());
    }

    println!("applied migrations to revert: [{:?}]", migrations_to_revert);
    revert(&session, dir_path, migrations_to_revert).await
}

async fn apply_migration(session: &Session, migration_path: &str) -> Result<()> {
    let query = file_contents(migration_path)?;

    // unable to pass queries in a single request.
    // batch request doesn't accept create table queries.
    // so splitting for now
    let queries: Vec<&str> = query
        .split(';')
        .filter(|q| !q.is_empty())
        .collect();

    println!("applying migration: {migration_path}");

    for query in queries {
        session.query_unpaged(query, &[]).await?;
    }

    println!("migration applied. Successfully");
    Ok(())
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
