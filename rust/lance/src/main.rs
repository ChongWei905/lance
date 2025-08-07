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

#[tokio::main]
async fn main() {
    println!("开始执行测试...");
    let args: Vec<String> = env::args().collect();
    println!("程序名称: {}", args[0]);
    let uri = args[1].as_str();
    let overall_start = Instant::now();
    test_full_scan_from_lance_file(uri).await;
    let overall_end = Instant::now();
    println!("{:?}", overall_end - overall_start);
    println!("测试完成！");
}

pub async fn test_full_scan_from_lance_file(
    file_uri_str: &str,
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
    let batches = tokio::task::spawn_blocking(move || {
        file_reader
            .read_stream_projected_blocking(
                ReadBatchParams::RangeFull,
                8192,
                Some(ReaderProjection::from_whole_schema(file_reader.schema(), file_reader.metadata().version())),
                FilterExpression::no_filter(),
            )
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap()
    })
        .await
        .unwrap();
    batches.iter().for_each(|batch| {
        for column in batch.columns() {
            // 访问数组的长度会触发数据加载
            let _len = column.len();

            for i in 0..column.len() {
                let _ = column.is_valid(i);
            }
        }
    });
}
