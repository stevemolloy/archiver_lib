use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::chrono::{DateTime, Local, NaiveDateTime, Utc};
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use tokio;

const DB_TYPE: &str = "postgresql://hdb_viewer";
const DB_USER: &str = "2tQXXVJtax+QLj61tg1Zxg+AByTLTt526AHcM+XmVCVW";
const DB_URL: &str = "timescaledb.maxiv.lu.se";
const DB_PORT: &str = "15432";

#[derive(Parser)]
#[command(about, long_about=None)]
struct Cli {
    searchstr: String,

    #[arg(short, long)]
    start: NaiveDateTime,

    #[arg(short, long)]
    end: NaiveDateTime,
}

#[derive(Debug)]
struct ArchiverAttr {
    id: i32,
    name: String,
    table: String,
}

struct ArchiverData {
    name: String,
    time: Vec<DateTime<Utc>>,
    data: Vec<f64>,
}

impl ArchiverData {
    fn get_taurus_format(&self) -> String {
        let mut result: String = Default::default();
        result += format!("\"# DATASET= {}\"\n", self.name).as_str();
        result += format!("\"# SNAPSHOT_TIME=\"\n").as_str();

        for (date, val) in self.time.iter().zip(self.data.iter()) {
            result += format!(
                "{}, {}\n",
                date.with_timezone(&Local).format("%Y-%m-%d_%H:%M:%S.%f"),
                val
            )
            .as_str();
        }

        return result;
    }

    fn write_taurus_file(self, fname: &str) {
        let string_to_write = self.get_taurus_format();
        let mut file = File::create(fname).expect("Could not open file");

        file.write_all(string_to_write.as_bytes())
            .expect("Could not write to file");
    }
}

async fn get_ids_and_tables(
    searchstr: String,
    pool: &Pool<Postgres>,
) -> Result<Vec<ArchiverAttr>, sqlx::Error> {
    let rows = sqlx::query(
        format!(
            "SELECT att_conf_id, att_name, table_name FROM att_conf
            WHERE att_name ~ '{searchstr}' ORDER BY att_conf_id"
        )
        .as_str(),
    )
    .fetch_all(pool)
    .await?;

    let result = rows
        .iter()
        .map(|r| ArchiverAttr {
            id: r.get::<i32, _>(0),
            name: r.get::<String, _>(1),
            table: r.get::<String, _>(2),
        })
        .collect::<Vec<ArchiverAttr>>();

    Ok(result)
}

async fn get_single_attr_data(
    attr: &ArchiverAttr,
    start: &DateTime<Local>,
    end: &DateTime<Local>,
    pool: &Pool<Postgres>,
) -> Result<ArchiverData, sqlx::Error> {
    let rows = sqlx::query(
        format!(
            "SELECT * FROM {} WHERE att_conf_id = {} AND data_time
            BETWEEN '{}' AND '{}' ORDER BY data_time",
            attr.table, attr.id, start, end
        )
        .as_str(),
    )
    .fetch_all(pool)
    .await?;

    let mut result = ArchiverData {
        name: attr.name.clone(),
        time: vec![],
        data: vec![],
    };

    for row in rows {
        result.time.push(row.get(1));
        result.data.push(row.get(2));
    }

    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let cli = Cli::parse();

    let database = "accelerator";
    let db_names = HashMap::from([("accelerator", "hdb_machine")]);

    let db_conn_str = format!(
        "{DB_TYPE}:{DB_USER}@{DB_URL}:{DB_PORT}/{db_name}",
        db_name = db_names[database]
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_conn_str)
        .await?;

    let attrs = get_ids_and_tables(cli.searchstr.to_string(), &pool).await?;

    let start = cli.start.and_local_timezone(Local).unwrap();
    let end = cli.end.and_local_timezone(Local).unwrap();

    for (i, attr) in attrs.iter().enumerate() {
        let res = get_single_attr_data(attr, &start, &end, &pool).await?;
        res.write_taurus_file(format!("archive_data_{:04}.dat", i).as_str());
    }

    Ok(())
}
