use std::collections::HashMap;
use std::time::Instant;
use std::sync::Arc;
use arrow_array::{RecordBatch, UInt32Array};
use futures::StreamExt;
use object_store::path::Path;
use rand::{thread_rng, Rng};
use lance_core::cache::LanceCache;
use lance_datagen::{array, gen, ByteCount, RowCount};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_encoding::version::LanceFileVersion;
use lance_file::v2::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_file::v2::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::ReadBatchParams;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;

#[tokio::main]
async fn main() {
    let batch = generate_and_write_data().await;
    write_data_to_file("./lance_v2_0.lance", batch.clone(), LanceFileVersion::V2_0).await;
    write_data_to_file("./lance_v2_1.lance", batch, LanceFileVersion::V2_1).await;
    let uint_vec: Vec<u32> = generate_random_numbers();
    println!("v2_0:");
    read_data_from_file("./lance_v2_0.lance", uint_vec.clone()).await;
    println!("v2_1:");
    read_data_from_file("./lance_v2_1.lance", uint_vec).await;
}

pub async fn read_data_from_file(
    uri: &str,
    uint_vec: Vec<u32>,
) {
    let overall_start = Instant::now();
    test_full_scan_from_lance_file(uri, uint_vec).await;
    let overall_end = Instant::now();
    println!("read time: {:?}", overall_end - overall_start);

    let overall_start = Instant::now();
    let empty_vec: Vec<u32> = Vec::new();
    test_full_scan_from_lance_file(uri, empty_vec).await;
    let overall_end = Instant::now();
    println!("read time: {:?}", overall_end - overall_start);
}

pub async fn generate_and_write_data() -> RecordBatch {
    gen()
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .anon_col(array::rand_utf8(ByteCount::from(10), false))
        .into_batch_rows(RowCount::from(1_000_000))
        .unwrap()
}

fn generate_random_numbers() -> Vec<u32> {
    let mut rng = thread_rng();
    let mut numbers: Vec<u32> =
        (0..20000)
            .map(|_| rng.gen_range(1..=1000000))
            .collect();
    numbers.sort();
    numbers
}
pub async fn write_data_to_file(
    file_uri_str: &str,
    batch: RecordBatch,
    version: LanceFileVersion,
) {
    let write_schema = batch.schema();
    let storage_options: HashMap<String, String> = HashMap::new();
    let object_params = ObjectStoreParams {
        storage_options: Some(storage_options),
        ..Default::default()
    };
    let (obj_store, path) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        &file_uri_str,
        &object_params,
    ).await.unwrap();
    let obj_store = Arc::new(obj_store);
    let obj_writer = obj_store.create(&path).await.unwrap();
    let mut file_writer_v2_0 = FileWriter::try_new(obj_writer, batch.schema().as_ref().try_into().unwrap(), FileWriterOptions {
        format_version: Some(version),
        ..Default::default()
    }).unwrap();
    file_writer_v2_0.write_batch(&batch).await.unwrap();
    file_writer_v2_0.finish().await.unwrap();
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
            let len_ = b.num_rows();

        }
        }
    }
}
