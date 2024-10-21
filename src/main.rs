use std::env::args;
use std::fs::{create_dir, File};
use std::path::Path;

const ARG_KEY_PATH: &str = "-p";
const ARG_KEY_DB_URL: &str = "-u";
const ENV_KEY_PATH: &str = "SCYLLADB_MIGRATE_DIR_PATH";
const ENV_KEY_DB_URL: &str = "SCYLLADB_MIGRATE_DB_URL";

fn main() -> Result<(), String> {
    let args: Vec<String> = args().collect();
    if args.len() < 2 {
        _ = help();
        return Err("Insufficient number of parameters".into());
    }

    let mut dir_path = arg_or_env(&args, ARG_KEY_PATH, ENV_KEY_PATH);
    if dir_path == "" {
        dir_path = ".".to_string()
    }
    let dir_path = dir_path;

    let db_url = arg_or_env(&args, ARG_KEY_DB_URL, ENV_KEY_DB_URL);

    let command = &args[1];
    match command.as_str() {
        "generate" => generate(args, dir_path.as_str()),
        _ => help()
    }
}

fn generate(args: Vec<String>, dir_path: &str) -> Result<(), String> {
    if args.len() < 3 {
        return Err("Insufficient number of parameters".into())
    }

    let name = args.last().unwrap(); //Should never crash as
    let date = chrono::Local::now();
    let formatted = date.format("%Y%m%d%H%M%S");
    let subdirectory_path = format!("{dir_path}/{formatted}_{name}");
    let subdirectory_path = subdirectory_path.as_str();

    let dir = Path::new(dir_path);
    if !dir.is_dir() {
        return Err(format!("Dir [{dir_path}] does not exist, or is not a directory"))
    }

    if let Err(err) = create_dir(subdirectory_path) {
        return Err(err.to_string())
    }

    let up = format!("{subdirectory_path}/up.cql");
    let down = format!("{subdirectory_path}/down.cql");

    if let Err(err) = File::create(up) {
        return Err(err.to_string())
    }
    if let(Err(err)) = File::create(down) {
        return Err(err.to_string())
    }

    Ok(())
}

fn help() -> Result<(), String> {
    println!("Usage: abc <command> [options]
    Available commands:
        generate <name> (The last value is always supposed to be name)
        run

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
