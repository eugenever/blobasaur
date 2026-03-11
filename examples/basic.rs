use rand::{Rng, RngExt};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, AsyncConnectionConfig, RedisError};
use serde::{Deserialize, Serialize};
use std::time::{self, Duration};

use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacAddress {
    octets: [u8; 6],
}

impl MacAddress {
    pub fn generate() -> Self {
        let mut octets = [0u8; 6];
        rand::rng().fill_bytes(&mut octets);

        // Set locally administered bit (bit 1), clear multicast bit (bit 0).
        octets[0] = (octets[0] | 0b0000_0010) & 0b1111_1110;

        Self { octets }
    }

    pub fn is_local(&self) -> bool {
        self.octets[0] & 0b0000_0010 != 0
    }

    pub fn is_unicast(&self) -> bool {
        self.octets[0] & 0b0000_0001 == 0
    }

    pub fn octets(&self) -> [u8; 6] {
        self.octets
    }
}

impl Display for MacAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let [a, b, c, d, e, f_] = self.octets;
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            a, b, c, d, e, f_
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WifiData {
    pub mac: String,
    pub lat: f64,
    pub lon: f64,
    pub acc: f64,
}

async fn pool(pool_size: usize) -> Result<Vec<MultiplexedConnection>, RedisError> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Duration::from_secs(30))
        .set_response_timeout(Duration::from_secs(30));
    let mut connections = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let c = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        connections.push(c);
    }
    Ok(connections)
}

async fn write(pool: &[MultiplexedConnection], n: usize) -> Result<Vec<WifiData>, RedisError> {
    let mut rng = rand::rng();
    let mut wds = Vec::with_capacity(n);
    (0..n).for_each(|_| {
        wds.push(WifiData {
            mac: MacAddress::generate().to_string(),
            lat: rng.random_range(51.0..61.0),
            lon: rng.random_range(28.0..38.0),
            acc: rng.random_range(10.0..140.0),
        });
    });

    let mut index_conn = 0;
    let mut handles = Vec::with_capacity(n);

    let st = std::time::Instant::now();

    for wd in wds.iter() {
        if index_conn >= pool.len() - 1 {
            index_conn = 0
        }
        let jh = tokio::task::spawn({
            let wd = wd.clone();
            let mut con = pool[index_conn].clone();
            async move {
                for _ in 0..1 {
                    let value = serde_json::to_vec(&wd).map_err(|e| RedisError::from(e))?;
                    let _: () = con.set(wd.mac.as_str(), value).await.unwrap();
                }
                Ok::<_, RedisError>(())
            }
        });
        handles.push(jh);
        index_conn += 1;
    }
    for jh in handles {
        let _ = jh.await.unwrap()?;
    }

    println!("Duration write {n} objects: {:?}", st.elapsed());

    Ok(wds)
}

async fn read(pool: &[MultiplexedConnection], wds: &[WifiData]) -> Result<(), RedisError> {
    let mut index_conn = 0;
    let mut handles = Vec::with_capacity(wds.len());

    let st = time::Instant::now();

    for wd in wds.iter() {
        if index_conn >= pool.len() - 1 {
            index_conn = 0
        }
        let jh = tokio::task::spawn({
            let wd = wd.clone();
            let mut con = pool[index_conn].clone();
            async move {
                for _ in 0..1 {
                    let bytes: Vec<u8> = con.get(wd.mac.as_str()).await?;
                    let _wd_form_redis: WifiData = serde_json::from_slice(&bytes)?;
                }
                Ok::<_, RedisError>(())
            }
        });
        handles.push(jh);
        index_conn += 1;
    }
    for jh in handles {
        let _ = jh.await.unwrap()?;
    }

    println!("Duration read {} objects: {:?}", wds.len(), st.elapsed());

    Ok(())
}

// cargo run --example basic --release
#[tokio::main]
async fn main() -> Result<(), RedisError> {
    let p = pool(5).await?;
    let n = 15_000;
    let wds = write(&p, n).await?;
    read(&p, &wds).await?;
    Ok(())
}
