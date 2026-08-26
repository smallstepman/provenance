#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
use provenance_core::QueryEngine;
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
use provenance_indexing_backend::test_support;
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
use std::hint::black_box;

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
type BenchmarkIndex = provenance_indexing_backend::BackendIndex<test_support::TestModel>;

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
fn backend_name() -> &'static str {
    if cfg!(feature = "sqlite") {
        "sqlite"
    } else if cfg!(feature = "duckdb") {
        "duckdb"
    } else if cfg!(feature = "doltlite") {
        "doltlite"
    } else if cfg!(feature = "turso") {
        "turso"
    } else if cfg!(feature = "redb") {
        "redb"
    } else if cfg!(feature = "heed") {
        "heed"
    } else if cfg!(feature = "mnestic") {
        "mnestic"
    } else {
        "lbug"
    }
}

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
fn open_index() -> BenchmarkIndex {
    BenchmarkIndex::in_memory().expect("open projection index")
}

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
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

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
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

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
criterion_group!(backends, bench_rebuild, bench_explain);
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
criterion_main!(backends);

#[cfg(not(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
)))]
fn main() {}
