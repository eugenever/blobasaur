use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};

// cargo run --release --package blobasaur --example streams_xgroup
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:6379")?;

    // NOTE: Even if the redis-rs docs say we can reuse/clone multiplexed_async_connection,
    //       for reading/writing on a stream, it's better to have a different connection (otherwise, read returns none)
    let stream_name = "stream-c04";

    println!();

    // -- Writer Task
    let mut con_writer = client.get_multiplexed_async_connection().await?;
    let writer_handle = tokio::spawn(async move {
        println!("WRITER - started");
        let st = std::time::Instant::now();
        for i in 0..100_000 {
            let _id: String = con_writer
                .xadd(stream_name, "*", &[("val", &i.to_string())])
                .await
                .unwrap();
        }
        println!("WRITER - finished, {:?}", st.elapsed());
    });

    // -- XREADGROUP group-01
    let group_01 = "group_01";
    let mut con_group_01 = client.get_multiplexed_async_connection().await?;
    let group_create_res: Result<(), _> = con_group_01
        .xgroup_create_mkstream(stream_name, group_01, "0")
        .await;
    if let Err(err) = group_create_res {
        println!("XGROUP - group '{group_01}' already exists, skipping creation. Cause: {err}");
    } else {
        println!("XGROUP - group '{group_01}' created successfully.");
    }

    // Consumer 01
    let reader_handle = tokio::spawn(async move {
        let consumer = "consumer_01";
        let options = StreamReadOptions::default()
            .count(1)
            .block(0)
            .group(group_01, consumer);

        let mut count = 0;
        let st = std::time::Instant::now();
        loop {
            let result: Option<StreamReadReply> = con_group_01
                .xread_options(&[stream_name], &[">"], &options)
                .await
                .expect("Failed to read stream");

            if let Some(reply) = result {
                for stream_key in reply.keys {
                    for stream_id in stream_key.ids {
                        /*
                            println!(
                                "XREADGROUP - {group_01} - {consumer} - read: id: {} - fields: {:?}",
                                stream_id.id, stream_id.map
                            );
                        */
                        let res: Result<(), _> = con_group_01
                            .xack(stream_name, group_01, &[&stream_id.id])
                            .await;
                        let _i: i64 = con_group_01
                            .xdel(stream_name, &[&stream_id.id])
                            .await
                            .unwrap();
                        if let Err(res) = res {
                            println!("XREADGROUP - ERROR ACK: {res}");
                        }
                    }
                    count += 1;
                }

                if count % 5000 == 0 {
                    println!(
                        "Concumer 1, processed {} messages, elapsed: {:?}",
                        count,
                        st.elapsed()
                    );
                }
            } else {
                println!(
                    "Concumer 1, read keys from group: {count}, elapsed: {:?}",
                    st.elapsed()
                );
                break;
            }
        }
    });

    // Consumer 02
    let mut con_group_01 = client.get_multiplexed_async_connection().await?;
    let reader_handle2 = tokio::spawn(async move {
        let consumer = "consumer_02";
        let options = StreamReadOptions::default()
            .count(1)
            .block(0)
            .group(group_01, consumer);

        let mut count = 0;
        let st = std::time::Instant::now();
        loop {
            let result: Option<StreamReadReply> = con_group_01
                .xread_options(&[stream_name], &[">"], &options)
                // .xread_options(&[stream_name], &[">"], &options)
                .await
                .expect("Failed to read stream");

            if let Some(reply) = result {
                for stream_key in reply.keys {
                    for stream_id in stream_key.ids {
                        let res: Result<(), _> = con_group_01
                            .xack(stream_name, group_01, &[&stream_id.id])
                            .await;
                        let _i: i64 = con_group_01
                            .xdel(stream_name, &[&stream_id.id])
                            .await
                            .unwrap();
                        if let Err(res) = res {
                            println!("XREADGROUP - ERROR ACK: {res}");
                        }
                    }
                    count += 1;
                }

                if count % 5000 == 0 {
                    println!(
                        "Concumer 2, processed {} messages, elapsed: {:?}",
                        count,
                        st.elapsed()
                    );
                }
            } else {
                println!(
                    "Concumer 2, read keys from group: {count}, elapsed: {:?}",
                    st.elapsed()
                );
                break;
            }
        }
    });

    // Consumer 02
    let mut con_group_01 = client.get_multiplexed_async_connection().await?;
    let reader_handle3 = tokio::spawn(async move {
        let consumer = "consumer_03";
        let options = StreamReadOptions::default()
            .count(1)
            .block(0)
            .group(group_01, consumer);

        let mut count = 0;
        let st = std::time::Instant::now();
        loop {
            let result: Option<StreamReadReply> = con_group_01
                .xread_options(&[stream_name], &[">"], &options)
                .await
                .expect("Failed to read stream");

            if let Some(reply) = result {
                for stream_key in reply.keys {
                    for stream_id in stream_key.ids {
                        let res: Result<(), _> = con_group_01
                            .xack(stream_name, group_01, &[&stream_id.id])
                            .await;
                        let _i: i64 = con_group_01
                            .xdel(stream_name, &[&stream_id.id])
                            .await
                            .unwrap();
                        if let Err(res) = res {
                            println!("XREADGROUP - ERROR ACK: {res}");
                        }
                    }
                    count += 1;
                }

                if count % 5000 == 0 {
                    println!(
                        "Concumer 3, processed {} messages, elapsed: {:?}",
                        count,
                        st.elapsed()
                    );
                }
            } else {
                println!(
                    "Concumer 3, read keys from group: {count}, elapsed: {:?}",
                    st.elapsed()
                );
                break;
            }
        }
    });

    // -- Wait for tasks to complete
    writer_handle.await?;
    reader_handle.await?;
    reader_handle2.await?;
    reader_handle3.await?;

    println!();

    // -- Clean up the stream
    let mut con = client.get_multiplexed_async_connection().await?;
    // Note: This will delete the stream and all attached groups
    let count: i32 = con.del(stream_name).await?;
    println!("Stream '{stream_name}' deleted ({count} key).");

    Ok(())
}
