use anyhow::Result;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::time::Instant;
use surrealdb::engine::local::RocksDb;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

#[derive(Debug, Serialize, Deserialize)]
struct NameBasic {
    nconst: String,
    primary_name: String,
    birth_year: Option<i32>,
    death_year: Option<i32>,
    primary_profession: String,
    known_for_titles: String,
}

static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();

    // // Create database connection
    let db_path = std::env::current_dir()?.join("imdb.surrealdb");
    // // if "imdb.surrealdb" exists, delete it
    // if std::path::Path::new(&db_path).exists() {
    //     std::fs::remove_dir_all(&db_path)?;
    // }
    let db = Surreal::new::<RocksDb>(db_path.clone()).await?;
    // let db = DB.connect::<Ws>("127.0.0.1:8000").await?;
    // println!("Database path: {:?}", db_path);
    // let db = Surreal::new::<Ws>("127.0.0.1:8000").await?;
    // db.signin(Root {
    //     username: "root".into(),
    //     password: "root".into(),
    // })
    // .await?;
    // Select namespace and database
    db.use_ns("benchmark").use_db("benchmark").await?;

    let csv_path = std::env::current_dir()?.join("../name.basics.tsv");
    println!("CSV path: {:?}", csv_path);

    // Count total rows first
    let row_count = ReaderBuilder::new()
        .delimiter(b'\t')
        .from_path(&csv_path)?
        .into_records()
        .count();
    println!("Total rows to process: {}", row_count);

    // Create reader again for actual processing
    let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_path(csv_path)?;

    let mut count = 0;
    // let mut batch: Vec<NameBasic> = Vec::with_capacity(row_count);
    println!("Processing records...");

    let tx = db.transaction().await?;
    // Process each record
    for result in rdr.records() {
        if count > 1000000 {
            break;
        }
        let record = result?;

        let birth_year = match &record[2] {
            "\\N" => None,
            year => Some(year.parse::<i32>()?),
        };

        let death_year = match &record[3] {
            "\\N" => None,
            year => Some(year.parse::<i32>()?),
        };
        // Create record in SurrealDB
        let name = NameBasic {
            nconst: record[0].to_string(),
            primary_name: record[1].to_string(),
            birth_year,
            death_year,
            primary_profession: record[4].to_string(),
            known_for_titles: record[5].to_string(),
        };

        // Insert using the nconst as the record ID
        // let created: Option<NameBasic> = db.create("name_basic").content(name).await?;
        // batch.push(name);
        (&tx)
            .create::<Option<NameBasic>>("name_basic")
            .content(name)
            .await?;
        count += 1;
        if count % 100_000 == 0 {
            println!("Processed {} NameBasics", count);
        }
    }

    // Single transaction at the end
    println!("Starting database insert...");
    // tx.commit().await?;

    let duration = start.elapsed();
    println!("Successfully imported {} records in {:?}", count, duration);

    Ok(())
}
