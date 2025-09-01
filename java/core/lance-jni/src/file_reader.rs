use std::collections::HashMap;
use std::mem;
use std::ops::Range;
use std::ptr::null;
use std::sync::{Arc, Mutex};

use crate::utils::to_rust_map;
use crate::{
    error::{Error, Result},
    traits::IntoJava,
    utils::JvmRef,
    JNIEnvExt, RT,
};
use arrow::{array::RecordBatchReader, ffi::{FFI_ArrowSchema, to_ffi}, ffi_stream::FFI_ArrowArrayStream};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::compute::concat_batches;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, StreamExt};
use jni::objects::JMap;
use jni::{
    errors::Error as JniError,
    objects::{GlobalRef, JByteArray, JObject, JString, JValue},
    sys::{jint, jlong},
    JNIEnv, JavaVM,
};
use lance::io::ObjectStore;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_encoding::{
    decoder::{DecoderPlugins, FilterExpression},
    EncodingsIo,
};
use lance_file::v2::reader::{FileReader, FileReaderOptions, FileReaderBuilder, FileReaderIo, ReaderProjection};
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
    ReadBatchParams,
};
use object_store::path::Path;
use lance_core::ArrowResult;
use snafu::location;

pub const NATIVE_READER: &str = "nativeFileReaderHandle";

pub struct CallbackReader {
    jvm: Arc<Mutex<JvmRef>>,
    input_stream: GlobalRef,
    shutdown: bool,
}

impl std::fmt::Debug for CallbackReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CallbackReader(shutdown={})", self.shutdown)
    }
}

impl CallbackReader {
    pub fn new(env: &mut JNIEnv<'_>, input_stream: JObject) -> Result<Self> {
        let jvm = JvmRef::new(env.get_java_vm()?.get_java_vm_pointer());
        let input_stream = env.new_global_ref(input_stream)?;
        Ok(Self {
            jvm: Arc::new(Mutex::new(jvm)),
            input_stream,
            shutdown: false,
        })
    }
}

impl EncodingsIo for CallbackReader {
    fn submit_request(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        _priority: u64,
    ) -> BoxFuture<'static, lance::Result<Vec<Bytes>>> {
        let jvm = self.jvm.clone();
        let input_stream = self.input_stream.clone();
        async move {
            let byte_buffers = tokio::task::spawn_blocking(move || {
                let jvm = {
                    let jvm_ptr = jvm.lock().unwrap();
                    unsafe { JavaVM::from_raw(jvm_ptr.val) }
                }?;

                let mut jvm_env = jvm.attach_current_thread()?;

                let mut byte_buffers = Vec::with_capacity(ranges.len());
                for range in ranges {
                    let res = jvm_env.call_method(
                        input_stream.as_obj(),
                        "readAt",
                        "(JJ)[B",
                        &[
                            JValue::Long(range.start as i64),
                            JValue::Long((range.end - range.start) as i64),
                        ],
                    )?;
                    let res_arr: JByteArray<'_> = res.l()?.into();
                    let res_bytes = jvm_env.convert_byte_array(&res_arr)?;
                    byte_buffers.push(Bytes::from(res_bytes));
                }
                std::result::Result::<_, JniError>::Ok(byte_buffers)
            })
                .await
                .unwrap();
            Ok(byte_buffers.map_err(|e| {
                lance::Error::io(format!("Error reading bytes: {}", e), location!())
            })?)
        }
            .boxed()
    }
}

#[async_trait]
impl FileReaderIo for CallbackReader {
    async fn file_size(&self) -> std::result::Result<u64, lance::Error> {
        let jvm = self.jvm.clone();
        let input_stream = self.input_stream.clone();
        let file_size = tokio::task::spawn_blocking(move || {
            let jvm = {
                let jvm_ptr = jvm.lock().unwrap();
                unsafe { JavaVM::from_raw(jvm_ptr.val) }
            }?;

            let mut jvm_env = jvm.attach_current_thread()?;
            let res = jvm_env.call_method(input_stream.as_obj(), "fileSize", "()J", &[])?;
            let file_size: jlong = res.j()?;
            std::result::Result::<_, JniError>::Ok(file_size as u64)
        })
            .await
            .unwrap();
        Ok(file_size.map_err(|e| {
            lance::Error::io(format!("Error getting file size: {}", e), location!())
        })?)
    }

    fn metadata_read_size(&self) -> u64 {
        4096
    }
}


#[derive(Clone, Debug)]
pub struct BlockingFileReader {
    pub(crate) inner: Arc<FileReader>,
}

impl BlockingFileReader {
    pub fn create(file_reader: Arc<FileReader>) -> Self {
        Self { inner: file_reader }
    }

    pub fn open_stream(
        &self,
        batch_size: u32,
        read_batch_params: ReadBatchParams,
        reader_projection: Option<ReaderProjection>,
        filter_expression: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(self.inner.read_stream_projected_blocking(
            read_batch_params,
            batch_size,
            reader_projection,
            filter_expression,
        )?)
    }

    pub fn schema(&self) -> Result<SchemaRef> {
        Ok(Arc::new(self.inner.schema().as_ref().into()))
    }

    pub fn num_rows(&self) -> u64 {
        self.inner.num_rows()
    }
}

impl IntoJava for BlockingFileReader {
    fn into_java<'local>(self, env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
        attach_native_reader(env, self)
    }
}

fn attach_native_reader<'local>(
    env: &mut JNIEnv<'local>,
    reader: BlockingFileReader,
) -> Result<JObject<'local>> {
    let j_reader = create_java_reader_object(env)?;
    unsafe { env.set_rust_field(&j_reader, NATIVE_READER, reader) }?;
    Ok(j_reader)
}

fn create_java_reader_object<'a>(env: &mut JNIEnv<'a>) -> Result<JObject<'a>> {
    let res = env.new_object("com/lancedb/lance/file/LanceFileReader", "()V", &[])?;
    Ok(res)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_openNative<'local>(
    mut env: JNIEnv<'local>,
    _reader_class: JObject,
    file_uri: JString,
    storage_options_obj: JObject,
) -> JObject<'local> {
    ok_or_throw!(env, inner_open(&mut env, file_uri, storage_options_obj))
}

fn inner_open<'local>(
    env: &mut JNIEnv<'local>,
    file_uri: JString,
    storage_options_obj: JObject,
) -> Result<JObject<'local>> {
    let file_uri_str: String = env.get_string(&file_uri)?.into();
    let jmap = JMap::from_env(env, &storage_options_obj)?;
    let storage_options = to_rust_map(env, &jmap)?;
    let reader = RT.block_on(async move {
        let object_params = ObjectStoreParams {
            storage_options: Some(storage_options),
            ..Default::default()
        };
        let (obj_store, path) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &file_uri_str,
            &object_params,
        )
        .await?;
        let config = SchedulerConfig::max_bandwidth(&obj_store);
        let scan_scheduler = ScanScheduler::new(obj_store, config);

        let file_scheduler = scan_scheduler
            .open_file(&Path::parse(&path)?, &CachedFileSize::unknown())
            .await?;
        FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
    })?;

    let reader = BlockingFileReader::create(Arc::new(reader));

    reader.into_java(env)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_openNativeWithJavaIo<'local>(
    mut env: JNIEnv<'local>,
    _reader_class: JObject,
    input_stream: JObject,
    path_str_obj: JString,
) -> JObject<'local> {
    ok_or_throw!(
        env,
        inner_open_java_io(&mut env, input_stream, path_str_obj)
    )
}

fn inner_open_java_io<'local>(
    env: &mut JNIEnv<'local>,
    input_stream: JObject,
    path_str_obj: JString,
) -> Result<JObject<'local>> {
    let path_str: String = env.get_string(&path_str_obj)?.into();
    let path = Path::parse(&path_str)
        .map_err(|e| Error::input_error(format!("Invalid path string: {}", e)))?;
    let callback_reader = Arc::new(CallbackReader::new(env, input_stream)?);

    let reader = RT.block_on(async move {
        FileReaderBuilder::new_custom(callback_reader, path)
            .build()
            .await
    })?;

    let reader = BlockingFileReader::create(Arc::new(reader));

    reader.into_java(env)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_closeNative<'local>(
    mut env: JNIEnv<'local>,
    reader: JObject,
) -> JObject<'local> {
    let maybe_err =
        unsafe { env.take_rust_field::<_, _, BlockingFileReader>(reader, NATIVE_READER) };
    match maybe_err {
        Ok(_) => {}
        // We were already closed, do nothing
        Err(jni::errors::Error::NullPtr(_)) => {}
        Err(err) => Error::from(err).throw(&mut env),
    }
    JObject::null()
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_numRowsNative(
    mut env: JNIEnv<'_>,
    reader: JObject,
) -> jlong {
    match inner_num_rows(&mut env, reader) {
        Ok(num_rows) => num_rows,
        Err(e) => {
            e.throw(&mut env);
            0
        }
    }
}

// If the reader is closed, the native handle will be null and we will get a JniError::NullPtr
// error when we call get_rust_field.  Translate that into a more meaningful error.
fn unwrap_reader<T>(val: std::result::Result<T, jni::errors::Error>) -> Result<T> {
    match val {
        Ok(val) => Ok(val),
        Err(jni::errors::Error::NullPtr(_)) => Err(Error::io_error(
            "FileReader has already been closed".to_string(),
        )),
        err => Ok(err?),
    }
}

fn inner_num_rows(env: &mut JNIEnv<'_>, reader: JObject) -> Result<jlong> {
    let reader = unsafe { env.get_rust_field::<_, _, BlockingFileReader>(reader, NATIVE_READER) };
    let reader = unwrap_reader(reader)?;
    Ok(reader.num_rows() as i64)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_populateSchemaNative(
    mut env: JNIEnv,
    reader: JObject,
    schema_addr: jlong,
) {
    ok_or_throw_without_return!(env, inner_populate_schema(&mut env, reader, schema_addr));
}

fn inner_populate_schema(env: &mut JNIEnv, reader: JObject, schema_addr: jlong) -> Result<()> {
    let reader = unsafe { env.get_rust_field::<_, _, BlockingFileReader>(reader, NATIVE_READER) };
    let reader = unwrap_reader(reader)?;
    let schema = reader.schema()?;
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
    unsafe { std::ptr::write_unaligned(schema_addr as *mut FFI_ArrowSchema, ffi_schema) }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_readAllNative(
    mut env: JNIEnv<'_>,
    reader: JObject,
    batch_size: jint,
    projected_names: JObject,
    selection_ranges: JObject,
    stream_addr: jlong,
) {
    let result = (|| -> Result<()> {
        let mut read_parameter = ReadBatchParams::default();
        let mut reader_projection: Option<ReaderProjection> = None;
        // We get reader here not from env.get_rust_field, because we need reader: MutexGuard<BlockingFileReader> has no relationship with the env lifecycle.
        // If we get reader from env.get_rust_field, we can't use env (can't borrow again) until we drop the reader.
        #[allow(unused_variables)]
        let reader = unsafe {
            let reader_ref = reader.as_ref();
            let ptr = env.get_field(reader_ref, NATIVE_READER, "J")?.j()?
                as *mut Mutex<BlockingFileReader>;
            let guard = env.lock_obj(reader_ref)?;
            if ptr.is_null() {
                return Err(Error::io_error(
                    "FileReader has already been closed".to_string(),
                ));
            }
            (*ptr).lock().unwrap()
        };

        let file_version = reader.inner.metadata().version();

        if !projected_names.is_null() {
            let schema = Schema::try_from(reader.schema()?.as_ref())?;
            let column_names: Vec<String> = env.get_strings(&projected_names)?;
            let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
            reader_projection = Some(ReaderProjection::from_column_names(
                file_version,
                &schema,
                names.as_slice(),
            )?);
        } else {
            reader_projection = Some(ReaderProjection::from_whole_schema(
                reader.inner.schema(),
                reader.inner.metadata().version()
            ));
        }

        if !selection_ranges.is_null() {
            let mut ranges: Vec<Range<u64>> = Vec::new();
            let jlist = env.get_list(&selection_ranges)?;
            let mut j_list_iter = jlist.iter(&mut env)?;

            loop {
                let item = j_list_iter.next(&mut env)?;
                if item.is_none() {
                    break; // End of the list
                }

                let item_obj = item.unwrap();
                if item_obj.is_null() {
                    continue;
                }

                let start_val = env
                    .call_method(&item_obj, "getStart", "()I", &[])
                    .and_then(|v| v.i())?;
                let end_val = env
                    .call_method(&item_obj, "getEnd", "()I", &[])
                    .and_then(|v| v.i())?;

                if start_val < 0 || end_val < 0 {
                    return Err(Error::input_error(format!(
                        "Invalid range values (negative): start={}, end={}",
                        start_val, end_val
                    )));
                }
                if start_val > end_val {
                    return Err(Error::input_error(format!(
                        "Invalid range (start > end): start={}, end={}",
                        start_val, end_val
                    )));
                }

                ranges.push(Range {
                    start: start_val as u64,
                    end: end_val as u64,
                });

                env.delete_local_ref(item_obj)?;
            }
            read_parameter = ReadBatchParams::Ranges(ranges.into_boxed_slice().into());
        }
        inner_read_all(
            &reader,
            batch_size,
            read_parameter,
            reader_projection,
            FilterExpression::no_filter(),
            stream_addr,
        )
    })();
    if let Err(e) = result {
        e.throw(&mut env);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_takeNative(
    mut env: JNIEnv<'_>,
    reader: JObject,
    batch_size: jint,
    projected_names: JObject,
    row_indices: JObject,
    stream_addr: jlong,
) {
    let result = (|| -> Result<()> {
        let mut read_parameter = ReadBatchParams::default();
        let mut reader_projection: Option<ReaderProjection> = None;
        // We get reader here not from env.get_rust_field, because we need reader: MutexGuard<BlockingFileReader> has no relationship with the env lifecycle.
        // If we get reader from env.get_rust_field, we can't use env (can't borrow again) until we drop the reader.
        #[allow(unused_variables)]
        let reader = unsafe {
            let reader_ref = reader.as_ref();
            let ptr = env.get_field(reader_ref, NATIVE_READER, "J")?.j()?
                as *mut Mutex<BlockingFileReader>;
            let guard = env.lock_obj(reader_ref)?;
            if ptr.is_null() {
                return Err(Error::io_error(
                    "FileReader has already been closed".to_string(),
                ));
            }
            (*ptr).lock().unwrap()
        };

        let file_version = reader.inner.metadata().version();

    if !projected_names.is_null() {
        let schema = Schema::try_from(reader.schema().unwrap().as_ref()).unwrap();
        let column_names: Vec<String> = env.get_strings(&projected_names).unwrap();
        let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        reader_projection = Some(ReaderProjection::from_column_names(
            file_version,
            &schema,
            names.as_slice(),
        ).unwrap());
    } else {
        reader_projection = Some(ReaderProjection::from_whole_schema(
            reader.inner.schema(),
            reader.inner.metadata().version()
        ));
    }

        if row_indices.is_null() {
            return Err(Error::input_error(
                "Row indices cannot be null".to_string(),
            ));
        }
        let expected_row_indecis: Vec<u32> = unsafe { mem::transmute(env.get_integers(&row_indices)?) };
        read_parameter = ReadBatchParams::Indices(expected_row_indecis.into());

        inner_read_all(
            &reader,
            batch_size,
            read_parameter,
            reader_projection,
            FilterExpression::no_filter(),
            stream_addr,
        )
    })();
    if let Err(e) = result {
        e.throw(&mut env);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_takeDataNative(
    mut env: JNIEnv<'_>,
    reader: JObject,
    batch_size: jint,
    projected_names: JObject,
    row_indices: JObject,
    arrow_array_addr: jlong,
    arrow_schema_addr: jlong,
) {
    let result = (|| -> Result<()> {
        let mut read_parameter = ReadBatchParams::default();
        let mut reader_projection: Option<ReaderProjection> = None;
        // We get reader here not from env.get_rust_field, because we need reader: MutexGuard<BlockingFileReader> has no relationship with the env lifecycle.
        // If we get reader from env.get_rust_field, we can't use env (can't borrow again) until we drop the reader.
        #[allow(unused_variables)]
        let reader = unsafe {
            let reader_ref = reader.as_ref();
            let ptr = env.get_field(reader_ref, NATIVE_READER, "J")?.j()?
                as *mut Mutex<BlockingFileReader>;
            let guard = env.lock_obj(reader_ref)?;
            if ptr.is_null() {
                return Err(Error::io_error(
                    "FileReader has already been closed".to_string(),
                ));
            }
            (*ptr).lock().unwrap()
        };

        let file_version = reader.inner.metadata().version();

        if !projected_names.is_null() {
            let schema = Schema::try_from(reader.schema()?.as_ref())?;
            let column_names: Vec<String> = env.get_strings(&projected_names)?;
            let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
            reader_projection = Some(ReaderProjection::from_column_names(
                file_version,
                &schema,
                names.as_slice(),
            )?);
        }

        if row_indices.is_null() {
            return Err(Error::input_error(
                "Row indices cannot be null".to_string(),
            ));
        }
        let expected_row_indecis: Vec<u32> = unsafe { mem::transmute(env.get_integers(&row_indices)?) };
        read_parameter = ReadBatchParams::Indices(expected_row_indecis.into());

        inner_read_all_data(
            &reader,
            batch_size,
            read_parameter,
            reader_projection,
            FilterExpression::no_filter(),
            arrow_array_addr,
            arrow_schema_addr,
        )
    })();
    if let Err(e) = result {
        e.throw(&mut env);
    }
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_file_LanceFileReader_readAllDataNative(
    mut env: JNIEnv<'_>,
    reader: JObject,
    batch_size: jint,
    projected_names: JObject,
    selection_ranges: JObject,
    arrow_array_addr: jlong,
    arrow_schema_addr: jlong,
) {
    let result = (|| -> Result<()> {
        let mut read_parameter = ReadBatchParams::default();
        let mut reader_projection: Option<ReaderProjection> = None;
        // We get reader here not from env.get_rust_field, because we need reader: MutexGuard<BlockingFileReader> has no relationship with the env lifecycle.
        // If we get reader from env.get_rust_field, we can't use env (can't borrow again) until we drop the reader.
        #[allow(unused_variables)]
        let reader = unsafe {
            let reader_ref = reader.as_ref();
            let ptr = env.get_field(reader_ref, NATIVE_READER, "J")?.j()?
                as *mut Mutex<BlockingFileReader>;
            let guard = env.lock_obj(reader_ref)?;
            if ptr.is_null() {
                return Err(Error::io_error(
                    "FileReader has already been closed".to_string(),
                ));
            }
            (*ptr).lock().unwrap()
        };

        let file_version = reader.inner.metadata().version();

        if !projected_names.is_null() {
            let schema = Schema::try_from(reader.schema()?.as_ref())?;
            let column_names: Vec<String> = env.get_strings(&projected_names)?;
            let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
            reader_projection = Some(ReaderProjection::from_column_names(
                file_version,
                &schema,
                names.as_slice(),
            )?);
        }

        if !selection_ranges.is_null() {
            let mut ranges: Vec<Range<u64>> = Vec::new();
            let jlist = env.get_list(&selection_ranges)?;
            let mut j_list_iter = jlist.iter(&mut env)?;

            loop {
                let item = j_list_iter.next(&mut env)?;
                if item.is_none() {
                    break; // End of the list
                }

                let item_obj = item.unwrap();
                if item_obj.is_null() {
                    continue;
                }

                let start_val = env
                    .call_method(&item_obj, "getStart", "()I", &[])
                    .and_then(|v| v.i())?;
                let end_val = env
                    .call_method(&item_obj, "getEnd", "()I", &[])
                    .and_then(|v| v.i())?;

                if start_val < 0 || end_val < 0 {
                    return Err(Error::input_error(format!(
                        "Invalid range values (negative): start={}, end={}",
                        start_val, end_val
                    )));
                }
                if start_val > end_val {
                    return Err(Error::input_error(format!(
                        "Invalid range (start > end): start={}, end={}",
                        start_val, end_val
                    )));
                }

                ranges.push(Range {
                    start: start_val as u64,
                    end: end_val as u64,
                });

                env.delete_local_ref(item_obj)?;
            }
            read_parameter = ReadBatchParams::Ranges(ranges.into_boxed_slice().into());
        }
        inner_read_all_data(
            &reader,
            batch_size,
            read_parameter,
            reader_projection,
            FilterExpression::no_filter(),
            arrow_array_addr,
            arrow_schema_addr,
        )
    })();
    if let Err(e) = result {
        e.throw(&mut env);
    }
}

fn inner_read_all(
    reader: &BlockingFileReader,
    batch_size: jint,
    read_batch_params: ReadBatchParams,
    reader_projection: Option<ReaderProjection>,
    filter_expression: FilterExpression,
    stream_addr: jlong,
) -> Result<()> {
    let arrow_stream = reader.open_stream(
        batch_size as u32,
        read_batch_params,
        reader_projection,
        filter_expression,
    )?;
    let ffi_stream = FFI_ArrowArrayStream::new(arrow_stream);
    unsafe { std::ptr::write_unaligned(stream_addr as *mut FFI_ArrowArrayStream, ffi_stream) }
    Ok(())
}

fn inner_read_all_data(
    reader: &BlockingFileReader,
    batch_size: jint,
    read_batch_params: ReadBatchParams,
    reader_projection: Option<ReaderProjection>,
    filter_expression: FilterExpression,
    arrow_array_addr: jlong,
    arrow_schema_addr: jlong,
) -> Result<()> {
    let merged_batch = RT.block_on(async move {
        let mut arrow_stream = reader.inner.read_stream_projected(
            read_batch_params,
            batch_size as u32,
            16,
            ReaderProjection::from_whole_schema(reader.inner.schema(), reader.inner.metadata().version()),
            filter_expression,
        ).unwrap();
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let batch_result = arrow_stream.next() {
            match batch_result.await { batch => {
                if batch.is_none() {
                    break;
                }
                batches.push(batch.unwrap().unwrap());
            }
            }
        }
        let write_schema = batches[0].schema();
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        concat_batches(&write_schema, batch_refs).unwrap()
    });

    let struct_array: StructArray = merged_batch.into();
    let array_data = struct_array.to_data();

    let (arrow_array, arrow_schema) = to_ffi(&array_data).unwrap();
    unsafe {
        std::ptr::write_unaligned(arrow_schema_addr as *mut FFI_ArrowSchema, arrow_schema);
        std::ptr::write_unaligned(arrow_array_addr as *mut FFI_ArrowArray, arrow_array);
    }
    Ok(())
}
