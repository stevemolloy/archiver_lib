use sqlx::postgres::PgPoolOptions;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use tokio;

const DB_TYPE: &str = "postgresql://hdb_viewer";
const DB_USER: &str = "2tQXXVJtax+QLj61tg1Zxg+AByTLTt526AHcM+XmVCVW";
const DB_URL: &str = "timescaledb.maxiv.lu.se";
const DB_PORT: &str = "15432";

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
    fn get_taurus_format(self) -> String {
        let mut result: String = Default::default();
        result += format!("\"# DATASET= {}\"", self.name).as_str();
        todo!()
    }

    fn write_taurus_file(self, fname: &str) {
        todo!()
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
    start: &str,
    end: &str,
    pool: &Pool<Postgres>,
) -> Result<ArchiverData, sqlx::Error> {
    let rows = sqlx::query(
        format!(
            "SELECT * FROM {} WHERE att_conf_id = {} AND data_time
            BETWEEN '{}' AND '{}'
            ORDER BY data_time",
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
        let a = row.get::<DateTime<Utc>, _>(1);
        let b = row.get::<f64, _>(2);
        result.time.push(a);
        result.data.push(b);
    }

    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
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

    let attrs = get_ids_and_tables("r3.*dia.*dcct.*inst.*".to_string(), &pool).await?;

    let start = "2024-05-06T00:00:00".to_string();
    let stop = "2024-05-06T00:01:00".to_string();

    for (i, attr) in attrs.iter().enumerate() {
        let res = get_single_attr_data(attr, &start, &stop, &pool).await?;
        println!("{}", res.name);
        for (t, v) in res.time.iter().zip(res.data) {
            println!("{}: {}", t, v);
        }
    }

    Ok(())
}
