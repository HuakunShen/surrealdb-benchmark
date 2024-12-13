import Surreal from "surrealdb";
import { records } from "./csv";
// import data from "../name_basics.json";

// Define the database configuration interface
interface DbConfig {
  url: string;
  namespace: string;
  database: string;
}

// Define the default database configuration
const DEFAULT_CONFIG: DbConfig = {
  url: "ws://127.0.0.1:8000",
  namespace: "benchmark",
  database: "benchmark",
};

// Define the function to get the database instance
export async function getDb(config: DbConfig = DEFAULT_CONFIG): Promise<Surreal> {
  const db = new Surreal();
  
  try {
      await db.connect(config.url);
      await db.signin({
        username: "root",
        password: "root",
      });
      await db.use({ namespace: config.namespace, database: config.database });
    return db;
  } catch (err) {
    console.error("Failed to connect to SurrealDB:", err instanceof Error ? err.message : String(err));
    await db.close();
    throw err;
  }
}

const db = await getDb();

// const result = await db.query("SELECT * FROM name_basic");
// console.log("data", data);

// const data = await Bun.file("../name_basics.json").json()

const BATCH_SIZE = 1000;

await db.query("DELETE name_basic;")
console.log("begin processing in batches");
const start = performance.now()

for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    let batchQuery = '';
    
    for (const row of batch) {
        batchQuery += `CREATE name_basic CONTENT ${JSON.stringify(row)};`
    }

    await db.queryRaw(`
        BEGIN TRANSACTION;
        ${batchQuery}
        COMMIT TRANSACTION;
    `);
    
    console.log(`Processed ${Math.min(i + BATCH_SIZE, records.length)} of ${records.length} records`);
    if (i > 1_000_000) {
      break;
    }
}

const end = performance.now()
console.log(`Total time: ${end - start}ms`)
