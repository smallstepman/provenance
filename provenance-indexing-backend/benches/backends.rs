#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
))]
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
))]
use provenance_core::QueryEngine;
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
))]
use provenance_indexing_backend::test_support;
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
))]
use std::hint::black_box;

#[cfg(any(
    all(feature = "sqlite", feature = "duckdb"),
    all(feature = "sqlite", feature = "doltlite"),
    all(feature = "sqlite", feature = "turso"),
    all(feature = "sqlite", feature = "lbug"),
    all(feature = "duckdb", feature = "doltlite"),
    all(feature = "duckdb", feature = "turso"),
    all(feature = "duckdb", feature = "lbug"),
    all(feature = "doltlite", feature = "turso"),
    all(feature = "doltlite", feature = "lbug"),
    all(feature = "turso", feature = "lbug"),
))]
compile_error!("database backend features are mutually exclusive");

#[cfg(feature = "sqlite")]
type BenchmarkIndex = provenance_indexing_backend::SqliteIndex<test_support::TestModel>;

#[cfg(feature = "duckdb")]
type BenchmarkIndex = provenance_indexing_backend::DuckDbIndex<test_support::TestModel>;

#[cfg(feature = "doltlite")]
type BenchmarkIndex = provenance_indexing_backend::DoltliteIndex<test_support::TestModel>;

#[cfg(feature = "turso")]
type BenchmarkIndex = provenance_indexing_backend::TursoIndex<test_support::TestModel>;

#[cfg(feature = "lbug")]
type BenchmarkIndex = provenance_indexing_backend::LbugIndex<test_support::TestModel>;

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
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
))]
criterion_group!(backends, bench_rebuild, bench_explain);
#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
))]
criterion_main!(backends);

#[cfg(not(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
)))]
fn main() {}
