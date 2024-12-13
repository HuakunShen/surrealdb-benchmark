// https://datasets.imdbws.com/name.basics.tsv.gz
use anyhow::{Context, Result};
use csv::ReaderBuilder;
use rusqlite::{params, Connection};
use std::time::Instant;

fn main() -> Result<()> {
    // Create SQLite database and table
    let mut conn = Connection::open("imdb.db")?;

    let start = Instant::now();
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM name_basics;",
        [],
        |row| row.get(0)
    )?;
    println!("Total records: {}", count);
    let duration = start.elapsed();
    println!("Duration: {:?}", duration);
    Ok(())
}
