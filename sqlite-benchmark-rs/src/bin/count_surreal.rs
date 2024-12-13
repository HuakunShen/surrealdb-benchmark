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

    // // Create database connection
    let db_path = std::env::current_dir()?.join("imdb.surrealdb");
    // if "imdb.surrealdb" exists, delete it
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
    let query = "select count() from name_basic group ALL;";
    let start = Instant::now();
    let result = db.query(query).await?;
    println!("Result: {:#?}", result);
    // let csv_path = std::env::current_dir()?.join("../name.basics.tsv");
    // println!("CSV path: {:?}", csv_path);
    let duration = start.elapsed();
    println!("Duration: {:?}", duration);
    Ok(())
}
