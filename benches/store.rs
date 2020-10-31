use std::path::{Path, PathBuf};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use foo::Blob;

fn generate_bench_file(name: &str, human_size: &str) -> std::io::Result<PathBuf> {
    let out_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(name);

    if !out_path.exists() {
        eprintln!("generating {}...", out_path.display());

        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "head -c {} </dev/urandom > {}",
                human_size,
                out_path.display()
            ))
            .status()?;

        eprintln!("process exited with {}", status);
        if !status.success() {
            panic!("failed to create {} for hashing benchmarks", name);
        }
    }

    Ok(out_path)
}

fn hash_blob_object(c: &mut Criterion) {
    let small = generate_bench_file("small_file", "15K").unwrap();
    let medium = generate_bench_file("medium_file", "33M").unwrap();
    let large = generate_bench_file("large_file", "1G").unwrap();

    let mut group = c.benchmark_group("Small file (15K)");
    group.bench_function("Blob::from_path", |b| {
        b.iter_with_large_drop(|| Blob::from_path(&small).unwrap())
    });
    group.bench_function("Blob::from_reader (File)", |b| {
        b.iter_batched(
            || std::fs::File::open(&small).unwrap(),
            |f| Blob::from_reader(f, false).unwrap(),
            BatchSize::LargeInput,
        )
    });
    group.finish();

    let mut group = c.benchmark_group("Medium file (33M)");
    group.bench_function("Blob::from_path", |b| {
        b.iter_with_large_drop(|| Blob::from_path(&medium).unwrap())
    });
    group.bench_function("Blob::from_reader (File)", |b| {
        b.iter_batched(
            || std::fs::File::open(&medium).unwrap(),
            |f| Blob::from_reader(f, false).unwrap(),
            BatchSize::LargeInput,
        )
    });
    group.finish();

    let mut group = c.benchmark_group("Large file (1G)");
    group.bench_function("Blob::from_path", |b| {
        b.iter_with_large_drop(|| Blob::from_path(&large).unwrap())
    });
    group.bench_function("Blob::from_reader (File)", |b| {
        b.iter_batched(
            || std::fs::File::open(&large).unwrap(),
            |f| Blob::from_reader(f, false).unwrap(),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = hash_blob_object
}

criterion_main!(benches);
