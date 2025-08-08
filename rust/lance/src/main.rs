use std::collections::HashMap;
use std::env;
use std::time::Instant;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use arrow_array::UInt32Array;
use futures::StreamExt;
use object_store::path::Path;
use lance_core::ArrowResult;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::v2::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::ReadBatchParams;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use rand::{thread_rng, Rng};


#[tokio::main]
async fn main() {
    println!("开始执行测试...");
    let args: Vec<String> = env::args().collect();
    println!("程序名称: {}", args[0]);
    let uri = args[1].as_str();
    let mode = args[2].as_str();
    if (mode == "randomaccess") {
        let indices_file = args[3].as_str();
        let indices_line = std::fs::read_to_string(indices_file).unwrap();
        let indices = indices_line.trim();

        let uint_vec: Vec<u32> = indices.split(',')
            .map(|s| s.parse::<u32>().unwrap())
            .collect();
        let overall_start = Instant::now();
        test_full_scan_from_lance_file(uri, uint_vec).await;
        let overall_end = Instant::now();
        println!("read time: {:?}", overall_end - overall_start);
    }
    if (mode == "fullscan") {
        let overall_start = Instant::now();
        let empty_vec: Vec<u32> = Vec::new();
        test_full_scan_from_lance_file(uri, empty_vec).await;
        let overall_end = Instant::now();
        println!("read time: {:?}", overall_end - overall_start);
    }

    println!("测试完成！");
}

fn generate_random_numbers() -> Vec<u32> {
    let mut rng = thread_rng();
    let mut numbers: Vec<u32> =
        (0..200)
        .map(|_| rng.gen_range(1..=10000))
        .collect();
    numbers.sort();
    numbers
}


pub async fn test_full_scan_from_lance_file(
    file_uri_str: &str,
    take_rows_array: Vec<u32>,
) {

    let object_params = ObjectStoreParams {
        storage_options: Some(HashMap::new()),
        ..Default::default()
    };
    let (obj_store, path) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        &file_uri_str,
        &object_params,
    ).await.unwrap();
    let config = SchedulerConfig::max_bandwidth(&obj_store);
    let scan_scheduler = ScanScheduler::new(obj_store, config);

    let file_scheduler = scan_scheduler
        .open_file(&Path::parse(&path).unwrap(), &CachedFileSize::unknown())
        .await.unwrap();
    let file_reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    ).await.unwrap();
    let read_batch_params: ReadBatchParams;
    if (take_rows_array.len() > 0) {
        read_batch_params = ReadBatchParams::Indices(UInt32Array::from(take_rows_array));
    } else {
        read_batch_params = ReadBatchParams::RangeFull;
    }
    let mut arrow_stream = file_reader
        .read_stream_projected(
            read_batch_params,
            8192,
            16,
            ReaderProjection::from_whole_schema(file_reader.schema(), file_reader.metadata().version()),
            FilterExpression::no_filter(),
        ).unwrap();

    while let batch_result = arrow_stream.next() {
        match batch_result.await { batch => {
            if (batch.is_none()) {
                break;
            }
            let b = batch.unwrap().unwrap();
            let _len = b.num_rows();
        }
        }
    }

}
