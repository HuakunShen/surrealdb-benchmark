// https://datasets.imdbws.com/name.basics.tsv.gz
use anyhow::{Context, Result};
use csv::ReaderBuilder;
use rusqlite::{params, Connection};
use std::time::Instant;

fn main() -> Result<()> {
    let start = Instant::now();

    // Create SQLite database and table
    let mut conn = Connection::open("imdb.db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS name_basics (
            nconst TEXT PRIMARY KEY,
            primary_name TEXT NOT NULL,
            birth_year INTEGER,
            death_year INTEGER,
            primary_profession TEXT,
            known_for_titles TEXT
        )",
        [],
    )?;

    // Begin transaction for faster inserts
    let tx = conn.transaction()?;

    // Prepare the insert statement
    let mut stmt: rusqlite::Statement<'_> = tx.prepare(
        "INSERT INTO name_basics (
            nconst, primary_name, birth_year, death_year, primary_profession, known_for_titles
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    let csv_path = std::env::current_dir()?.join("../name.basics.tsv");
    println!("CSV path: {:?}", csv_path);
    // Create CSV reader
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t') // IMDb files use tab as delimiter
        .from_path(csv_path)?;

    let mut count = 0;

    // Process each record
    for result in rdr.records() {
        let record = result?;

        let birth_year = match &record[2] {
            "\\N" => None,
            year => Some(year.parse::<i32>().context("Failed to parse birth year")?),
        };

        let death_year = match &record[3] {
            "\\N" => None,
            year => Some(year.parse::<i32>().context("Failed to parse death year")?),
        };

        stmt.execute(params![
            &record[0], // nconst
            &record[1], // primary_name
            birth_year, // birth_year
            death_year, // death_year
            &record[4], // primary_profession
            &record[5], // known_for_titles
        ])?;

        count += 1;
        if count % 100_000 == 0 {
            println!("Processed {} records", count);
        }
    }
    // Drop the prepared statement before committing to release the borrow
    drop(stmt);

    // Commit the transaction
    tx.commit()?;

    let duration = start.elapsed();
    println!("Successfully imported {} records in {:?}", count, duration);

    Ok(())
}
