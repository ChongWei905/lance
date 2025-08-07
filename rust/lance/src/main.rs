use std::collections::HashMap;
use std::env;
use std::time::Instant;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use futures::StreamExt;
use object_store::path::Path;
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
    let mut arrow_stream = file_reader.read_stream_projected(
        ReadBatchParams::RangeFull,
        8192,
        1,
        ReaderProjection::from_whole_schema(file_reader.schema(), file_reader.metadata().version()),
        FilterExpression::no_filter(),
    ).unwrap();
    while let batch_result = arrow_stream.next() {
        match batch_result.await { batch => {
            if (batch.is_none()) {
                break;
            }
            let b = batch.unwrap().unwrap();
            // 强制访问所有列的所有数据
            for column in b.columns() {
                // 访问数组的长度会触发数据加载
                let _len = column.len();

                // 对于某些数组类型，还需要访问实际数据
                match column.data_type() {
                    arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                        // 对于字符串数组，访问所有值
                        for i in 0..column.len() {
                            let _ = column.is_valid(i);
                        }
                    }
                    arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                        // 对于二进制数组，访问所有值
                        for i in 0..column.len() {
                            let _ = column.is_valid(i);
                        }
                    }
                    _ => {
                        // 对于其他类型，访问有效性掩码通常足够
                        for i in 0..column.len() {
                            let _ = column.is_valid(i);
                        }
                    }
                }
            }
        }
        }
    }
}
