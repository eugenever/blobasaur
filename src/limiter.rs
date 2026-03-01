use std::collections::HashMap;
use std::io;

use chrono::Datelike;
use chrono_tz::Europe::Moscow;
use miette::Result;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    task::JoinHandle,
    time::{Duration, interval},
};

use crate::config::LimiterConfig;

static LIMITER_FILE: &str = "./limiters/limiters.json";
static DEFAULT_LIMITER_BACKUP_DAYS: i64 = 30;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Limiter {
    pub tx: flume::Sender<LimiterMessage>,
    pub rx: flume::Receiver<LimiterMessage>,
}

pub enum LimiterMessage {
    GetLimit {
        api_key: String,
        tx: oneshot::Sender<u64>,
    },
    SetLimit {
        api_key: String,
        limit: u64,
    },
    Save,
    Load,
}

pub fn limiter_task(
    tx: flume::Sender<LimiterMessage>,
    rx: flume::Receiver<LimiterMessage>,
    limiter: Option<LimiterConfig>,
) -> Result<(JoinHandle<()>, JoinHandle<()>)> {
    let jh_limiter = tokio::spawn({
        let mut limiters: HashMap<String, u64> = HashMap::new();

        async move {
            while let Ok(message) = rx.recv_async().await {
                // Moscow time countdown
                let now_moscow = chrono::Utc::now().with_timezone(&Moscow);
                let day = now_moscow.day();
                let month = now_moscow.month();
                let year = now_moscow.year();

                match message {
                    LimiterMessage::GetLimit { api_key, tx } => {
                        let key = format!("[{:02}-{:02}-{}]::{}", day, month, year, api_key);

                        if let Some(limit) = limiters.get(&key) {
                            let incr_limit = limit + 1;
                            if let Err(_) = tx.send(incr_limit) {
                                tracing::error!("send limit for key '{}'", key);
                            }
                            // update limit
                            limiters.insert(key, incr_limit);
                        } else {
                            if let Err(_) = tx.send(1) {
                                tracing::error!("send limit for key '{}'", key);
                            }
                            limiters.insert(key, 1);
                        }
                    }
                    LimiterMessage::SetLimit { api_key, limit } => {
                        let key = format!("[{:02}-{:02}-{}]::{}", day, month, year, api_key);
                        limiters.insert(key, limit);
                    }
                    LimiterMessage::Save => {
                        let temp_limiters = limiters.clone();
                        for (key, _) in temp_limiters {
                            let date_part: Vec<&str> = key.split("::").collect();
                            if let Some(date_brackets) = date_part.first() {
                                let mut chars = date_brackets.chars();
                                chars.next();
                                chars.next_back();
                                let date_str = chars.as_str();
                                if let Ok(date) =
                                    chrono::NaiveDate::parse_from_str(date_str, "%d-%m-%Y")
                                {
                                    let backup_days = if let Some(l) = limiter.as_ref() {
                                        l.backup_days
                                    } else {
                                        DEFAULT_LIMITER_BACKUP_DAYS
                                    };
                                    let check_date =
                                        now_moscow - chrono::Duration::days(backup_days);
                                    if check_date.date_naive() > date {
                                        limiters.remove(&key);
                                    }
                                }
                            }
                        }

                        if let Err(e) = write_limiters_to_file(&limiters, LIMITER_FILE).await {
                            tracing::error!("save limiters to file: {}", e);
                        }
                    }
                    LimiterMessage::Load => match read_limiters_from_file(LIMITER_FILE).await {
                        Err(e) => tracing::error!("load limiters from file: {}", e),
                        Ok(Some(l)) => limiters = l,
                        Ok(None) => {
                            // no-op, file not exists
                        }
                    },
                }
            }
        }
    });

    let jh_save = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(10)); // 10 seconds
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(_) = tx.send(LimiterMessage::Save) {
                        tracing::error!("send Save limiter message");
                    }
                }
            }
        }
    });

    Ok((jh_limiter, jh_save))
}

pub async fn read_limiters_from_file(path: &str) -> io::Result<Option<HashMap<String, u64>>> {
    if tokio::fs::metadata(path).await.is_ok() {
        let mut file = File::open(path).await?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;
        let limiters: HashMap<String, u64> = serde_json::from_str(&contents)?;
        return Ok(Some(limiters));
    }
    return Ok(None);
}

pub async fn write_limiters_to_file(limiters: &HashMap<String, u64>, path: &str) -> io::Result<()> {
    let mut file = File::create(path).await?;
    let contents = serde_json::to_string_pretty(limiters)?;
    file.write_all(contents.as_bytes()).await?;
    Ok(())
}
