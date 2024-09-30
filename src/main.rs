mod dingostore;
use dingostore::DingoStore;
use std::time::Instant;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use tokio;

// LIMITATIONS:
// - Currently no compaction. This means that as writes increase, so will the # of SSTables and
// reads will slow down. Will implement this soon
// - Currently no Write Ahead Log (WAL), so upon death of the server, KVs are lost. I will
// implement this soon.
// - Keys must be of type u64. String keys are nice, but they lead to variable length keys, which
// can slow down reads a bit. I'm thinking about implementing them anyways and having a type switch
// to discern between which type of key is used.
// - Not aligned to SSD blocks. I wanted to finish implementing this but want to do more testing.
// - I haven't tested this as much as I'd like
// - Error handling and logging aren't ideal


fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ds = DingoStore::new("dingostore");
    let start_time = Instant::now();

    // Write a large amount of data
    println!("Writing data...");
    let total_writes = 100000;
    let mut total_write_time = std::time::Duration::new(0, 0);
    let mut key_value_pairs = Vec::with_capacity(total_writes);

    for i in 0..total_writes {
        let value_len = if i % 2 == 0 { 50 } else { 100 };
        let key = i as u64;
        let value = generate_random_string(value_len);
        key_value_pairs.push((key, value.clone()));
        
        let write_start = Instant::now();
        ds.insert(key, value);
        total_write_time += write_start.elapsed();
        
        if (i + 1) % 10000 == 0 {
            println!("Wrote {}/{} entries", i + 1, total_writes);
        }
    }

    let write_time = start_time.elapsed();

    // Read and verify data
    println!("Reading and verifying data...");
    let mut read_count = 0;
    let mut missing_count = 0;
    let mut incorrect_count = 0;
    let read_start_time = Instant::now();
    let total_reads = 100000;
    let mut total_read_time = std::time::Duration::new(0, 0);

    for (i, (key, expected_value)) in key_value_pairs.iter().enumerate().take(total_reads) {
        let read_start = Instant::now();
        match ds.get(*key) {
            Some(value) => {
                total_read_time += read_start.elapsed();
                if value == *expected_value {
                    read_count += 1;
                } else {
                    println!("Value mismatch for key {}: expected {} but got {}", key, expected_value, value);
                    incorrect_count += 1;
                }
            }
            None => {
                total_read_time += read_start.elapsed();
                missing_count += 1;
                println!("Key {} not found (missing count: {})", key, missing_count);
                // Break the loop after 100 consecutive missing keys
                if missing_count >= 100 {
                    println!("Breaking loop after 100 consecutive missing keys");
                    break;
                }
            }
        }
        if (i + 1) % 1000 == 0 || i < 100 {
            println!("Processed {}/{} entries, found: {}, missing: {}, incorrect: {}", i + 1, total_reads, read_count, missing_count, incorrect_count);
        }
    }

    let read_time = read_start_time.elapsed();
    println!("Read complete. Time elapsed: {:?}", read_time);
    println!("Average read time: {:?}", total_read_time / total_reads as u32);
    println!("Successfully read and verified {} out of {} entries", read_count, total_reads);
    println!("Missing entries: {}", missing_count);
    println!("Incorrect entries: {}", incorrect_count);

    let total_misses = missing_count + incorrect_count;
    let miss_percentage = (total_misses as f64 / total_reads as f64) * 100.0;
    println!("Total misses: {} ({:.2}% of total reads)", total_misses, miss_percentage);

    println!("Total operation time: {:?}", start_time.elapsed());

    // Print summary statistics
    println!("\nSummary Statistics:");
    println!("Total write time: {:?}", write_time);
    println!("Average write time: {:?}", total_write_time / total_writes as u32);
    println!("Total read time: {:?}", read_time);
    println!("Average read time: {:?}", total_read_time / total_reads as u32);
    Ok(())
}
