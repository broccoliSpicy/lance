// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{cmp::Ordering, collections::HashMap, ops::Range, sync::Arc};

use arrow::array::make_comparator;
use arrow_array::{Array, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef, Schema, SortOptions};
use arrow_select::concat::concat;
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use log::{debug, trace};
use tokio::sync::mpsc::{self, UnboundedSender};

use lance_core::{
    cache::{CapacityMode, FileMetadataCache},
    Result,
};
use lance_datagen::{array, gen, ArrayGenerator, RowCount, Seed};

use crate::{
    buffer::LanceBuffer,
    decoder::{
        BatchDecodeStream, ColumnInfo, CoreFieldDecoderStrategy, DecodeBatchScheduler,
        DecoderMessage, DecoderMiddlewareChain, FilterExpression, PageInfo,
    },
    encoder::{
        ColumnIndexSequence, CoreArrayEncodingStrategy, CoreFieldEncodingStrategy, EncodedPage,
        EncodingOptions, FieldEncoder, FieldEncodingStrategy,
    },
    encodings::logical::r#struct::SimpleStructDecoder,
    version::LanceFileVersion,
    EncodingsIo,
};

const MAX_PAGE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct SimulatedScheduler {
    data: Bytes,
}

impl SimulatedScheduler {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl EncodingsIo for SimulatedScheduler {
    fn submit_request(
        &self,
        ranges: Vec<Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>> {
        let data = ranges
            .into_iter()
            .map(|range| {
                if range.is_empty() {
                    // Some filesystems (e.g. S3 will return an error if an empty request is made and so we need to avoid those)
                    panic!("Empty request")
                }
                self.data.slice(range.start as usize..range.end as usize)
            })
            .collect();

        log::trace!("Scheduled request with priority {}", priority);
        std::future::ready(data)
            .map(move |data| {
                log::trace!("Decoded request with priority {}", priority);
                Ok(data)
            })
            .boxed()
    }
}

fn column_indices_from_schema_helper(
    fields: &[FieldRef],
    column_indices: &mut Vec<u32>,
    column_counter: &mut u32,
) {
    column_indices.push(*column_counter);
    *column_counter += 1;
    for field in fields {
        match field.data_type() {
            DataType::Struct(fields) => {
                column_indices_from_schema_helper(fields.as_ref(), column_indices, column_counter);
            }
            DataType::List(inner) => {
                column_indices_from_schema_helper(&[inner.clone()], column_indices, column_counter);
            }
            DataType::LargeList(inner) => {
                column_indices_from_schema_helper(&[inner.clone()], column_indices, column_counter);
            }
            DataType::FixedSizeList(inner, _) => {
                // FSL(primitive) does not get its own column
                column_indices.pop();
                *column_counter -= 1;
                column_indices_from_schema_helper(&[inner.clone()], column_indices, column_counter);
            }
            _ => {
                column_indices_from_schema_helper(&[], column_indices, column_counter);
            }
        }
    }
}

fn column_indices_from_schema(schema: &Schema) -> Vec<u32> {
    let mut column_indices = Vec::new();
    let mut column_counter = 0;
    column_indices_from_schema_helper(schema.fields(), &mut column_indices, &mut column_counter);
    column_indices
}

#[allow(clippy::too_many_arguments)]
async fn test_decode(
    num_rows: u64,
    batch_size: u32,
    schema: &Schema,
    column_infos: &[Arc<ColumnInfo>],
    expected: Option<Arc<dyn Array>>,
    io: Arc<dyn EncodingsIo>,
    schedule_fn: impl FnOnce(
        DecodeBatchScheduler,
        UnboundedSender<Result<DecoderMessage>>,
    ) -> (SimpleStructDecoder, BoxFuture<'static, ()>),
) {
    let lance_schema = lance_core::datatypes::Schema::try_from(schema).unwrap();
    let decode_and_validate = Arc::new(DecoderMiddlewareChain::new().add_strategy(Arc::new(
        CoreFieldDecoderStrategy {
            validate_data: true,
        },
    )));
    let cache = Arc::new(FileMetadataCache::with_capacity(
        128 * 1024 * 1024,
        CapacityMode::Bytes,
    ));
    let column_indices = column_indices_from_schema(schema);
    let decode_scheduler = DecodeBatchScheduler::try_new(
        &lance_schema,
        &column_indices,
        column_infos,
        &Vec::new(),
        num_rows,
        decode_and_validate,
        io,
        cache,
        &FilterExpression::no_filter(),
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::unbounded_channel();

    let (decoder, scheduler_fut) = schedule_fn(decode_scheduler, tx);

    scheduler_fut.await;

    let mut decode_stream = BatchDecodeStream::new(rx, batch_size, num_rows, decoder).into_stream();

    let mut offset = 0;
    while let Some(batch) = decode_stream.next().await {
        let batch = batch.task.await.unwrap();
        if let Some(expected) = expected.as_ref() {
            let actual = batch.column(0);
            let expected_size = (batch_size as usize).min(expected.len() - offset);
            let expected = expected.slice(offset, expected_size);
            assert_eq!(expected.data_type(), actual.data_type());
            if &expected != actual {
                if let Ok(comparator) = make_comparator(&expected, &actual, SortOptions::default())
                {
                    // We can't just assert_eq! because the error message is not very helpful.  This gives us a bit
                    // more information about where the mismatch is.
                    for i in 0..expected.len() {
                        if !matches!(comparator(i, i), Ordering::Equal) {
                            panic!(
                            "Mismatch at index {} expected {:?} but got {:?} first mismatch is expected {:?} but got {:?}",
                            i,
                            expected,
                            actual,
                            expected.slice(i, 1),
                            actual.slice(i, 1)
                        );
                        }
                    }
                } else {
                    // Some arrays (like the null type) don't have a comparator so we just re-run the normal comparison
                    // and let it assert
                    assert_eq!(&expected, actual);
                }
            }
        }
        offset += batch_size as usize;
    }
}

pub trait ArrayGeneratorProvider {
    fn provide(&self) -> Box<dyn ArrayGenerator>;
    fn copy(&self) -> Box<dyn ArrayGeneratorProvider>;
}
struct RandomArrayGeneratorProvider {
    field: Field,
}

impl ArrayGeneratorProvider for RandomArrayGeneratorProvider {
    fn provide(&self) -> Box<dyn ArrayGenerator> {
        array::rand_type(self.field.data_type())
    }

    fn copy(&self) -> Box<dyn ArrayGeneratorProvider> {
        Box::new(Self {
            field: self.field.clone(),
        })
    }
}

/// Given a field this will test the round trip encoding and decoding of random data
pub async fn check_round_trip_encoding_random(field: Field, metadata: HashMap<String, String>) {
    let field = field.with_metadata(metadata);
    let array_generator_provider = RandomArrayGeneratorProvider {
        field: field.clone(),
    };
    check_round_trip_encoding_generated(
        field,
        Box::new(array_generator_provider),
        LanceFileVersion::default(),
    )
    .await;
}

pub async fn check_round_trip_encoding_generated(
    field: Field,
    array_generator_provider: Box<dyn ArrayGeneratorProvider>,
    version: LanceFileVersion,
) {
    let lance_field = lance_core::datatypes::Field::try_from(&field).unwrap();
    for page_size in [4096, 1024 * 1024] {
        debug!("Testing random data with a page size of {}", page_size);
        let encoding_strategy = CoreFieldEncodingStrategy {
            array_encoding_strategy: Arc::new(CoreArrayEncodingStrategy { version }),
            version,
        };
        let encoder_factory = || {
            let mut column_index_seq = ColumnIndexSequence::default();
            let encoding_options = EncodingOptions {
                max_page_bytes: MAX_PAGE_BYTES,
                cache_bytes_per_column: page_size,
                keep_original_array: true,
            };
            encoding_strategy
                .create_field_encoder(
                    &encoding_strategy,
                    &lance_field,
                    &mut column_index_seq,
                    &encoding_options,
                )
                .unwrap()
        };

        // let array_generator_provider = RandomArrayGeneratorProvider{field: field.clone()};
        check_round_trip_field_encoding_random(
            encoder_factory,
            field.clone(),
            array_generator_provider.copy(),
        )
        .await
    }
}

fn supports_nulls(data_type: &DataType) -> bool {
    // We don't yet have nullability support for all types.  Don't test nullability for the
    // types we don't support.
    !matches!(data_type, DataType::Struct(_))
}

// The default will just test the full read
#[derive(Clone)]
pub struct TestCases {
    ranges: Vec<Range<u64>>,
    indices: Vec<Vec<u64>>,
    batch_size: u32,
    skip_validation: bool,
    max_page_size: Option<u64>,
    file_version: LanceFileVersion,
}

impl Default for TestCases {
    fn default() -> Self {
        Self {
            batch_size: 100,
            ranges: Vec::new(),
            indices: Vec::new(),
            skip_validation: false,
            max_page_size: None,
            file_version: LanceFileVersion::default(),
        }
    }
}

impl TestCases {
    pub fn with_range(mut self, range: Range<u64>) -> Self {
        self.ranges.push(range);
        self
    }

    pub fn with_indices(mut self, indices: Vec<u64>) -> Self {
        self.indices.push(indices);
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn without_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }

    pub fn with_max_page_size(mut self, max_page_size: u64) -> Self {
        self.max_page_size = Some(max_page_size);
        self
    }

    fn get_max_page_size(&self) -> u64 {
        self.max_page_size.unwrap_or(MAX_PAGE_BYTES)
    }

    pub fn with_file_version(mut self, version: LanceFileVersion) -> Self {
        self.file_version = version;
        self
    }
}

/// Given specific data and test cases we check round trip encoding and decoding
///
/// Note that the input `data` is a `Vec` to simulate multiple calls to `maybe_encode`.
/// In other words, these are multiple chunks of one long array and not multiple columns
/// in a record batch.  To feed a "record batch" you should first convert the record batch
/// to a struct array.
pub async fn check_round_trip_encoding_of_data(
    data: Vec<Arc<dyn Array>>,
    test_cases: &TestCases,
    metadata: HashMap<String, String>,
) {
    let example_data = data.first().expect("Data must have at least one array");
    let mut field = Field::new("", example_data.data_type().clone(), true);
    field = field.with_metadata(metadata);
    let lance_field = lance_core::datatypes::Field::try_from(&field).unwrap();
    for page_size in [4096, 1024 * 1024] {
        let encoding_strategy = CoreFieldEncodingStrategy {
            array_encoding_strategy: Arc::new(CoreArrayEncodingStrategy {
                version: test_cases.file_version,
            }),
            version: test_cases.file_version,
        };
        let mut column_index_seq = ColumnIndexSequence::default();
        let encoding_options = EncodingOptions {
            cache_bytes_per_column: page_size,
            max_page_bytes: test_cases.get_max_page_size(),
            keep_original_array: true,
        };
        let encoder = encoding_strategy
            .create_field_encoder(
                &encoding_strategy,
                &lance_field,
                &mut column_index_seq,
                &encoding_options,
            )
            .unwrap();
        check_round_trip_encoding_inner(encoder, &field, data.clone(), test_cases).await
    }
}

struct SimulatedWriter {
    page_infos: Vec<Vec<PageInfo>>,
    encoded_data: BytesMut,
}

impl SimulatedWriter {
    fn new(num_columns: u32) -> Self {
        let mut page_infos = Vec::with_capacity(num_columns as usize);
        page_infos.resize_with(num_columns as usize, Default::default);
        Self {
            page_infos,
            encoded_data: BytesMut::new(),
        }
    }

    fn write_buffer(&mut self, buffer: LanceBuffer) -> (u64, u64) {
        let offset = self.encoded_data.len() as u64;
        self.encoded_data.extend_from_slice(&buffer);
        let size = self.encoded_data.len() as u64 - offset;
        (offset, size)
    }

    fn write_page(&mut self, encoded_page: EncodedPage) {
        trace!("Encoded page {:?}", encoded_page);
        let page_buffers = encoded_page.array.data.into_buffers();
        let page_encoding = encoded_page.array.encoding;
        let buffer_offsets_and_sizes = page_buffers
            .into_iter()
            .map(|b| self.write_buffer(b))
            .collect::<Vec<_>>();

        let page_info = PageInfo {
            num_rows: encoded_page.num_rows,
            encoding: page_encoding,
            buffer_offsets_and_sizes: Arc::from(buffer_offsets_and_sizes.clone()),
        };

        let col_idx = encoded_page.column_idx as usize;
        self.page_infos[col_idx].push(page_info);
    }
}

/// This is the inner-most check function that actually runs the round trip and tests it
async fn check_round_trip_encoding_inner(
    mut encoder: Box<dyn FieldEncoder>,
    field: &Field,
    data: Vec<Arc<dyn Array>>,
    test_cases: &TestCases,
) {
    let mut writer = SimulatedWriter::new(encoder.num_columns());

    for arr in &data {
        for encode_task in encoder.maybe_encode(arr.clone()).unwrap() {
            let encoded_page = encode_task.await.unwrap();
            writer.write_page(encoded_page);
        }
    }

    for encode_task in encoder.flush().unwrap() {
        let encoded_page = encode_task.await.unwrap();
        writer.write_page(encoded_page);
    }

    let encoded_columns = encoder.finish().await.unwrap();
    let mut column_infos = Vec::new();
    for (col_idx, encoded_column) in encoded_columns.into_iter().enumerate() {
        for page in encoded_column.final_pages {
            writer.write_page(page);
        }

        let col_buffer_off_and_size = encoded_column
            .column_buffers
            .into_iter()
            .map(|b| writer.write_buffer(b))
            .collect::<Vec<_>>();

        let column_info = ColumnInfo::new(
            col_idx as u32,
            Arc::from(std::mem::take(&mut writer.page_infos[col_idx])),
            col_buffer_off_and_size,
            encoded_column.encoding,
        );

        column_infos.push(Arc::new(column_info));
    }

    let scheduler =
        Arc::new(SimulatedScheduler::new(writer.encoded_data.freeze())) as Arc<dyn EncodingsIo>;

    let schema = Schema::new(vec![field.clone()]);

    let num_rows = data.iter().map(|arr| arr.len() as u64).sum::<u64>();
    let concat_data = if test_cases.skip_validation {
        None
    } else {
        Some(concat(&data.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>()).unwrap())
    };

    /* 
    debug!("Testing full decode");
    let scheduler_copy = scheduler.clone();
    test_decode(
        num_rows,
        test_cases.batch_size,
        &schema,
        &column_infos,
        concat_data.clone(),
        scheduler_copy.clone(),
        |mut decode_scheduler, tx| {
            #[allow(clippy::single_range_in_vec_init)]
            let root_decoder = decode_scheduler.new_root_decoder_ranges(&[0..num_rows]);
            (
                root_decoder,
                async move {
                    decode_scheduler.schedule_range(
                        0..num_rows,
                        &FilterExpression::no_filter(),
                        tx,
                        scheduler_copy,
                    )
                }
                .boxed(),
            )
        },
    )
    .await;

    // Test range scheduling
    for range in &test_cases.ranges {
        debug!("Testing decode of range {:?}", range);
        let num_rows = range.end - range.start;
        let expected = concat_data
            .as_ref()
            .map(|concat_data| concat_data.slice(range.start as usize, num_rows as usize));
        let scheduler = scheduler.clone();
        let range = range.clone();
        test_decode(
            num_rows,
            test_cases.batch_size,
            &schema,
            &column_infos,
            expected,
            scheduler.clone(),
            |mut decode_scheduler, tx| {
                #[allow(clippy::single_range_in_vec_init)]
                let root_decoder = decode_scheduler.new_root_decoder_ranges(&[0..num_rows]);
                (
                    root_decoder,
                    async move {
                        decode_scheduler.schedule_range(
                            range,
                            &FilterExpression::no_filter(),
                            tx,
                            scheduler,
                        )
                    }
                    .boxed(),
                )
            },
        )
        .await;
    }
    */

    debug!("Testing decode of ranges {:?}", &test_cases.ranges);

    let num_rows: u64 = test_cases.ranges.iter().map(|range| range.end - range.start).sum();

    let slices: Vec<_> = test_cases.ranges.iter().filter_map(|range| {
        concat_data.as_ref().map(|data| data.slice(range.start as usize, (range.end - range.start) as usize))
    }).collect::<Vec<_>>();

    let expected = if !slices.is_empty() {
        Some(Arc::new(concat(&slices.iter().map(|s| s.as_ref()).collect::<Vec<_>>()).unwrap()) as Arc<dyn Array>)
    } else {
        None
    };

    let scheduler = scheduler.clone();
    let ranges = test_cases.ranges.clone();
    test_decode(
        num_rows,
        test_cases.batch_size,
        &schema,
        &column_infos,
        expected,
        scheduler.clone(),
        |mut decode_scheduler, tx| {
            #[allow(clippy::single_range_in_vec_init)]
            let root_decoder = decode_scheduler.new_root_decoder_ranges(&[0..num_rows]);
            (
                root_decoder,
                async move {
                    decode_scheduler.schedule_ranges(
                        &ranges,
                        &FilterExpression::no_filter(),
                        tx,
                        scheduler.clone(),
                    )
                }
                .boxed(),
            )
        },
    )
    .await;

    /* 
    // Test take scheduling
    for indices in &test_cases.indices {
        if indices.len() == 1 {
            debug!("Testing decode of index {}", indices[0]);
        } else {
            debug!(
                "Testing decode of {} indices spread across range [{}..{}]",
                indices.len(),
                indices[0],
                indices[indices.len() - 1]
            );
        }
        let num_rows = indices.len() as u64;
        let indices_arr = UInt64Array::from(indices.clone());
        let expected = concat_data
            .as_ref()
            .map(|concat_data| arrow_select::take::take(&concat_data, &indices_arr, None).unwrap());
        let scheduler = scheduler.clone();
        let indices = indices.clone();
        test_decode(
            num_rows,
            test_cases.batch_size,
            &schema,
            &column_infos,
            expected,
            scheduler.clone(),
            |mut decode_scheduler, tx| {
                let root_decoder = decode_scheduler.new_root_decoder_indices(&indices);
                (
                    root_decoder,
                    async move {
                        decode_scheduler.schedule_take(
                            &indices,
                            &FilterExpression::no_filter(),
                            tx,
                            scheduler,
                        )
                    }
                    .boxed(),
                )
            },
        )
        .await;
    }
    */
}

const NUM_RANDOM_ROWS: u32 = 10000;

/// Generates random data (parameterized by null rate, slicing, and # ingest batches)
/// and tests with that.
async fn check_round_trip_field_encoding_random(
    encoder_factory: impl Fn() -> Box<dyn FieldEncoder>,
    field: Field,
    array_generator_provider: Box<dyn ArrayGeneratorProvider>,
) {
    let null_rate = Some(0.5);
    // for null_rate in [None, Some(0.5), Some(1.0)] {
    for use_slicing in [false, true] {
        if null_rate != Some(1.0) && matches!(field.data_type(), DataType::Null) {
            continue;
        }

        let field = if null_rate.is_some() {
            if !supports_nulls(field.data_type()) {
                continue;
            }
            field.clone().with_nullable(true)
        } else {
            field.clone().with_nullable(false)
        };

        let test_cases = TestCases::default()
            //.with_range(0..500)
            //.with_range(100..1100)
            //.with_range(8000..8500)
            .with_range(1000..2044)
            .with_range(2045..2050);
            /* 
            .with_indices(vec![100])
            .with_indices(vec![0])
            .with_indices(vec![9999])
            .with_indices(vec![100, 1100, 5000])
            .with_indices(vec![1000, 2000, 3000])
            .with_indices(vec![2000, 2001, 2002, 2003, 2004])
            // Big take that spans multiple pages and generates multiple output batches
            .with_indices((100..500).map(|i| i * 3).collect::<Vec<_>>());
            */

        for num_ingest_batches in [1, 5, 10] {
            let rows_per_batch = NUM_RANDOM_ROWS / num_ingest_batches;
            let mut data = Vec::new();

            // Test both ingesting one big array sliced into smaller arrays and smaller
            // arrays independently generated.  These behave slightly differently.  For
            // example, a list array sliced into smaller arrays will have arrays whose
            // starting offset is not 0.
            if use_slicing {
                let mut generator = gen().anon_col(array_generator_provider.provide());
                if let Some(null_rate) = null_rate {
                    // The null generator is the only generator that already inserts nulls
                    // and attempting to do so again makes arrow-rs grumpy
                    if !matches!(field.data_type(), DataType::Null) {
                        generator.with_random_nulls(null_rate);
                    }
                }
                let all_data = generator
                    .into_batch_rows(RowCount::from(10000))
                    .unwrap()
                    .column(0)
                    .clone();
                let mut offset = 0;
                for _ in 0..num_ingest_batches {
                    data.push(all_data.slice(offset, rows_per_batch as usize));
                    offset += rows_per_batch as usize;
                }
            } else {
                for i in 0..num_ingest_batches {
                    let mut generator = gen()
                        .with_seed(Seed::from(i as u64))
                        .anon_col(array_generator_provider.provide());
                    if let Some(null_rate) = null_rate {
                        // The null generator is the only generator that already inserts nulls
                        // and attempting to do so again makes arrow-rs grumpy
                        if !matches!(field.data_type(), DataType::Null) {
                            generator.with_random_nulls(null_rate);
                        }
                    }
                    let arr = generator
                        .into_batch_rows(RowCount::from(rows_per_batch as u64))
                        .unwrap()
                        .column(0)
                        .clone();
                    data.push(arr);
                }
            }

            debug!(
                "Testing with {} rows divided across {} batches for {} rows per batch with null_rate={:?} and use_slicing={}",
                NUM_RANDOM_ROWS,
                num_ingest_batches,
                rows_per_batch,
                null_rate,
                use_slicing
            );
            check_round_trip_encoding_inner(encoder_factory(), &field, data, &test_cases).await
        }
    }
    // }
}
