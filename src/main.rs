use std::path::PathBuf;

use clap::{Parser, Subcommand};
use colored::*;
use miette::{Context, IntoDiagnostic, Result};

use lsmdb::api::StorageEngine;

#[derive(Parser, Debug)]
#[command(name = "lsmdb", version, about, long_about = None)]
struct Cli {
    /// Path to the storage directory (defaults to $HOME/.lsmdb)
    #[arg(global = true, short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Insert a key-value pair
    #[command(alias = "p")]
    Put { key: String, value: String },

    /// Get a value by key
    #[command(alias = "g")]
    Get { key: String },

    /// Update a value by key
    #[command(alias = "u")]
    Update { key: String, value: String },

    /// Remove key-value pair
    #[command(alias = "rm")]
    Remove { key: String },

    /// Clear everything
    Clear,

    /// Set or view config
    Config {
        /// Optional path to set as config dir
        path: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let config_path = cli.config.unwrap_or_else(default_config_path);

    let mut engine = StorageEngine::new(config_path.clone())
        .into_diagnostic()
        .wrap_err_with(|| format!("could not initialize storage at {:?}", config_path))?;

    match cli.command {
        Command::Put { key, value } => {
            engine
                .put(&key, &value)
                .into_diagnostic()
                .wrap_err_with(|| format!("failed to put key={key}"))?;

            success(&format!("put: {key} = {value}"));
        }
        Command::Get { key } => {
            match engine
                .get(&key)
                .into_diagnostic()
                .wrap_err_with(|| format!("failed to get key={key}"))?
            {
                Some(value) => info(&format!("{key} = {value}")),
                None => warn(&format!("key not found: {key}")),
            }
        }
        Command::Update { key, value } => {
            engine
                .update(&key, &value)
                .into_diagnostic()
                .wrap_err_with(|| format!("failed to update key={key}"))?;
            success(&format!("update: {key} = {value}"));
        }
        Command::Remove { key } => {
            engine
                .remove(&key)
                .into_diagnostic()
                .wrap_err_with(|| format!("failed to remove key={key}"))?;
            success(&format!("removed: {key}"));
        }
        Command::Clear => {
            engine
                .clear()
                .into_diagnostic()
                .wrap_err("failed to clear storage")?;
            success("store is cleared");
        }
        Command::Config { path } => {
            if let Some(p) = path {
                info(&format!("set config path: {:?}", p));
            } else {
                info(&format!("current config path: {:?}", config_path));
            }
        }
    }

    Ok(())
}

/// Resolve a default config path depending on OS
fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(".lsmdb"))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn success(msg: &str) {
    println!("{} {}", "✔".bright_green().bold(), msg.normal());
}

fn warn(msg: &str) {
    eprintln!("{} {}", "⚠".bright_yellow().bold(), msg.yellow());
}

fn info(msg: &str) {
    println!("{} {}", "➤".bright_cyan().bold(), msg.cyan());
}
