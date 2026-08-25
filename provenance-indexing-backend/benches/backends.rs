#[cfg(any(feature = "sqlite", feature = "duckdb"))]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
use provenance_core::QueryEngine;
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
use provenance_indexing_backend::test_support;
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
use std::hint::black_box;

#[cfg(all(feature = "sqlite", feature = "duckdb"))]
compile_error!("features `sqlite` and `duckdb` are mutually exclusive");

#[cfg(feature = "sqlite")]
type BenchmarkIndex = provenance_indexing_backend::SqliteIndex<test_support::TestModel>;

#[cfg(feature = "duckdb")]
type BenchmarkIndex = provenance_indexing_backend::DuckDbIndex<test_support::TestModel>;

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
fn backend_name() -> &'static str {
    if cfg!(feature = "sqlite") {
        "sqlite"
    } else {
        "duckdb"
    }
}

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
fn open_index() -> BenchmarkIndex {
    BenchmarkIndex::in_memory().expect("open projection index")
}

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
fn bench_rebuild(c: &mut Criterion) {
    let operations = vec![test_support::operation_with_nodes(256)];
    let mut group = c.benchmark_group("projection_rebuild");
    group.bench_function(backend_name(), |benchmark| {
        benchmark.iter_batched(
            open_index,
            |mut index| {
                index
                    .rebuild(&operations)
                    .expect("rebuild projection index");
                black_box(index);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
fn bench_explain(c: &mut Criterion) {
    let operations = vec![test_support::operation_with_nodes(256)];
    let query = test_support::explain_query();
    let mut index = open_index();
    index
        .rebuild(&operations)
        .expect("rebuild projection index");
    let mut group = c.benchmark_group("explain_query");
    group.bench_function(backend_name(), |benchmark| {
        benchmark.iter(|| black_box(index.execute(black_box(&query)).expect("backend query")));
    });
    group.finish();
}

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
criterion_group!(backends, bench_rebuild, bench_explain);
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
criterion_main!(backends);

#[cfg(not(any(feature = "sqlite", feature = "duckdb")))]
fn main() {}
