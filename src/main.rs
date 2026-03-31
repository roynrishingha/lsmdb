use anyhow::Result;
use colored::*;
use lsmdb::StorageEngine;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<()> {
    // Respect an explicit db path passed as the first argument so that users
    // can run multiple isolated databases side-by-side without touching source.
    let db_path: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".lsmdb")
        });

    print_banner(&db_path);

    let engine = StorageEngine::open(&db_path)
        .map_err(|e| anyhow::anyhow!("Could not open database at {:?}: {}", db_path, e))?;

    println!("{}", "Type 'help' for commands. 'exit' to quit.".dimmed());
    println!();

    loop {
        print!("{} ", "lsmdb>".cyan().bold());
        io::stdout().flush()?;

        let mut raw = String::new();
        if io::stdin().read_line(&mut raw)? == 0 {
            // EOF — e.g. pipe closed or Ctrl-D
            println!();
            break;
        }
        let input = raw.trim();

        if input.is_empty() {
            continue;
        }

        // Split into at most 3 parts: <cmd> <key> <value>
        // The third segment is the entire remainder, so values with spaces are allowed.
        let mut parts = input.splitn(3, ' ');
        let cmd = parts.next().unwrap_or("").to_lowercase();
        let arg1 = parts.next();
        let arg2 = parts.next();

        match cmd.as_str() {
            "exit" | "quit" => {
                println!("{}", "Goodbye!".green());
                break;
            }

            "help" => print_help(),

            "put" | "set" | "update" => match (arg1, arg2) {
                (Some(key), Some(val)) => {
                    let t = Instant::now();
                    match engine.put(key, val) {
                        Ok(_) => println!(
                            "{} {} {}",
                            "OK".green().bold(),
                            key.cyan(),
                            format!("({} µs)", t.elapsed().as_micros()).dimmed()
                        ),
                        Err(e) => eprintln!("{} {}", "error:".red().bold(), e),
                    }
                }
                _ => eprintln!("{}", "usage: put <key> <value>".yellow()),
            },

            "get" => match arg1 {
                Some(key) => {
                    let t = Instant::now();
                    match engine.get(key.as_bytes()) {
                        Ok(Some(val)) => {
                            let elapsed = t.elapsed().as_micros();
                            let val_str = String::from_utf8_lossy(&val);
                            println!("{} {} {}", key.cyan(), "→".dimmed(), val_str);
                            println!("{}", format!("  ({} µs)", elapsed).dimmed());
                        }
                        Ok(None) => println!("{}", "(nil)".dimmed()),
                        Err(e) => eprintln!("{} {}", "error:".red().bold(), e),
                    }
                }
                None => eprintln!("{}", "usage: get <key>".yellow()),
            },

            "delete" | "remove" | "rm" | "del" => match arg1 {
                Some(key) => {
                    let t = Instant::now();
                    match engine.remove(key) {
                        Ok(_) => println!(
                            "{} {} {}",
                            "OK".green().bold(),
                            key.cyan(),
                            format!("({} µs)", t.elapsed().as_micros()).dimmed()
                        ),
                        Err(e) => eprintln!("{} {}", "error:".red().bold(), e),
                    }
                }
                None => eprintln!("{}", "usage: delete <key>".yellow()),
            },

            "clear" => {
                eprint!("This will destroy ALL data. Confirm? [y/N] ");
                io::stderr().flush()?;
                let mut confirm = String::new();
                io::stdin().read_line(&mut confirm)?;
                if confirm.trim().eq_ignore_ascii_case("y") {
                    match engine.clear() {
                        Ok(_) => println!("{}", "Database wiped.".green().bold()),
                        Err(e) => eprintln!("{} {}", "error:".red().bold(), e),
                    }
                } else {
                    println!("{}", "Aborted.".dimmed());
                }
            }

            _ => {
                eprintln!("{} '{}'", "unknown command:".red(), cmd);
                eprintln!("Type '{}' for a list of commands.", "help".cyan());
            }
        }
    }

    Ok(())
}

fn print_banner(db_path: &PathBuf) {
    let border = "━".repeat(45);
    println!("{}", border.cyan().bold());
    println!(
        "{}  {}  {}",
        "┃".cyan().bold(),
        "  lsmdb  —  LSM-Tree Storage Engine  ".bold(),
        "┃".cyan().bold()
    );
    println!("{}", border.cyan().bold());
    println!(
        "  {} {}",
        "db path:".dimmed(),
        db_path.display().to_string().white()
    );
    println!();
}

fn print_help() {
    let cmds: &[(&str, &str)] = &[
        ("put <key> <value>", "Insert or update a key"),
        ("get <key>", "Retrieve a value by key"),
        ("delete <key>", "Soft-delete a key (tombstone)"),
        ("clear", "Destroy all data in the database"),
        ("help", "Show this message"),
        ("exit", "Quit"),
    ];

    println!("{}", "Commands:".bold().underline());
    for (syntax, desc) in cmds {
        println!("  {:<28} {}", syntax.cyan(), desc.dimmed());
    }
    println!();
    println!(
        "  {}  put, set, update are aliases. delete, remove, rm, del are aliases.",
        "Note:".yellow().bold()
    );
}
