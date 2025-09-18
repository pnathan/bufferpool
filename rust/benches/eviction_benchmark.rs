use bufferpool::bufferpool;
use bufferpool::framepool::{self, FramePool};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

/// Benchmark configuration for eviction strategy analysis
#[derive(Clone)]
pub struct BenchmarkConfig {
    pub name: &'static str,
    pub buffer_slots: usize,
    pub total_items: usize,
    pub access_pattern: AccessPattern,
    pub workload_type: WorkloadType,
}

#[derive(Clone)]
pub enum AccessPattern {
    Sequential,
    Random(Vec<u64>),
    Working(Vec<u64>), // Simulates working set locality
    LruWorst,          // Pattern designed to defeat LRU
}

#[derive(Clone)]
pub enum WorkloadType {
    ReadOnly,
    WriteHeavy(f64), // Percentage of write operations
    Mixed(f64, f64), // (read_pct, write_pct)
}

/// Performance metrics collected during benchmarking
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub strategy_name: String,
    pub buffer_slots: usize,
    pub total_operations: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub evictions: usize,
    pub writes_performed: usize,
    pub elapsed_nanos: u128,
}

impl PerformanceMetrics {
    pub fn hit_rate(&self) -> f64 {
        self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
    }

    pub fn operations_per_second(&self) -> f64 {
        (self.total_operations as f64) / (self.elapsed_nanos as f64 / 1_000_000_000.0)
    }

    pub fn avg_latency_nanos(&self) -> f64 {
        self.elapsed_nanos as f64 / self.total_operations as f64
    }
}

/// Eviction strategy function type alias
type EvictionStrategy<T> = fn(
    &[Option<framepool::PageFrame<T>>],
    &bufferpool::unique_stack::UniqueStack<u64>,
) -> Result<u64, bufferpool::BufferPoolErrors>;

/// Benchmark runner for eviction strategies
pub struct EvictionBenchmark {
    strategies: Vec<(&'static str, EvictionStrategy<String>)>,
    configs: Vec<BenchmarkConfig>,
}

impl Default for EvictionBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionBenchmark {
    pub fn new() -> Self {
        Self {
            strategies: vec![
                ("bottom_evictor", bufferpool::bottom_evictor),
                ("random_evictor", bufferpool::random_evictor),
            ],
            configs: vec![
                // Small buffer stress tests
                BenchmarkConfig {
                    name: "small_buffer_sequential",
                    buffer_slots: 3,
                    total_items: 100,
                    access_pattern: AccessPattern::Sequential,
                    workload_type: WorkloadType::ReadOnly,
                },
                BenchmarkConfig {
                    name: "small_buffer_random",
                    buffer_slots: 3,
                    total_items: 100,
                    access_pattern: AccessPattern::Random(
                        (0..1000).map(|_| fastrand::u64(0..100)).collect(),
                    ),
                    workload_type: WorkloadType::ReadOnly,
                },
                BenchmarkConfig {
                    name: "working_set_locality",
                    buffer_slots: 5,
                    total_items: 50,
                    access_pattern: AccessPattern::Working(
                        // 80% of accesses to 20% of data (80/20 rule)
                        Self::generate_working_set_pattern(50, 10, 1000),
                    ),
                    workload_type: WorkloadType::ReadOnly,
                },
                BenchmarkConfig {
                    name: "mixed_workload",
                    buffer_slots: 8,
                    total_items: 200,
                    access_pattern: AccessPattern::Random(
                        (0..500).map(|_| fastrand::u64(0..200)).collect(),
                    ),
                    workload_type: WorkloadType::Mixed(0.7, 0.3), // 70% read, 30% write
                },
                // Medium buffer tests
                BenchmarkConfig {
                    name: "medium_buffer_stress",
                    buffer_slots: 16,
                    total_items: 1000,
                    access_pattern: AccessPattern::Random(
                        (0..2000).map(|_| fastrand::u64(0..1000)).collect(),
                    ),
                    workload_type: WorkloadType::WriteHeavy(0.4), // 40% writes
                },
                // Large dataset tests
                BenchmarkConfig {
                    name: "large_dataset_scan",
                    buffer_slots: 32,
                    total_items: 10000,
                    access_pattern: AccessPattern::Sequential,
                    workload_type: WorkloadType::ReadOnly,
                },
            ],
        }
    }

    /// Generate access pattern that simulates working set locality
    fn generate_working_set_pattern(
        total_items: usize,
        working_set_size: usize,
        num_accesses: usize,
    ) -> Vec<u64> {
        let mut pattern = Vec::with_capacity(num_accesses);

        for _ in 0..num_accesses {
            // 80% chance to access working set, 20% chance to access other items
            if fastrand::f64() < 0.8 {
                pattern.push(fastrand::u64(0..working_set_size as u64));
            } else {
                pattern.push(fastrand::u64(working_set_size as u64..total_items as u64));
            }
        }

        pattern
    }

    /// Run benchmark for a specific strategy and configuration
    pub fn run_single_benchmark(
        &self,
        strategy_name: &str,
        strategy_fn: EvictionStrategy<String>,
        config: &BenchmarkConfig,
    ) -> PerformanceMetrics {
        let start_time = std::time::Instant::now();

        // Setup memory pool
        let mut mem_pool = framepool::MemPool::new();
        <framepool::MemPool<String> as FramePool<String>>::resize(
            &mut mem_pool,
            config.total_items as u64,
        )
        .unwrap();

        // Initialize data
        for i in 0..config.total_items {
            let data = Arc::new(format!("item_{i:06}"));
            <framepool::MemPool<String> as FramePool<String>>::put_frame(
                &mut mem_pool,
                i as u64,
                data,
            )
            .unwrap();
        }

        // Create buffer pool with the eviction strategy
        let mut buffer_pool: bufferpool::BufferPool<String> =
            bufferpool::BufferPool::new(config.buffer_slots, &mut mem_pool, strategy_fn);

        // Generate access sequence based on pattern
        let access_sequence = self.generate_access_sequence(config);

        let mut cache_hits = 0;
        let mut cache_misses = 0;
        let mut writes_performed = 0;

        // Execute the benchmark workload
        for &idx in &access_sequence {
            match &config.workload_type {
                WorkloadType::ReadOnly => {
                    if let Some(_page) = buffer_pool.get_page(idx) {
                        cache_hits += 1;
                    } else {
                        cache_misses += 1;
                    }
                }
                WorkloadType::WriteHeavy(write_ratio) => {
                    if fastrand::f64() < *write_ratio {
                        // Write operation
                        if let Some(page) = buffer_pool.get_page(idx) {
                            page.with_data(|data: &mut String| {
                                *data = format!("modified_item_{idx:06}");
                            });
                            writes_performed += 1;
                            cache_hits += 1;
                        } else {
                            cache_misses += 1;
                        }
                    } else {
                        // Read operation
                        if let Some(_page) = buffer_pool.get_page(idx) {
                            cache_hits += 1;
                        } else {
                            cache_misses += 1;
                        }
                    }
                }
                WorkloadType::Mixed(read_ratio, write_ratio) => {
                    let op_type = fastrand::f64();
                    if op_type < *read_ratio {
                        // Read operation
                        if let Some(_page) = buffer_pool.get_page(idx) {
                            cache_hits += 1;
                        } else {
                            cache_misses += 1;
                        }
                    } else if op_type < read_ratio + write_ratio {
                        // Write operation
                        if let Some(page) = buffer_pool.get_page(idx) {
                            page.with_data(|data: &mut String| {
                                *data = format!("modified_item_{idx:06}");
                            });
                            writes_performed += 1;
                            cache_hits += 1;
                        } else {
                            cache_misses += 1;
                        }
                    }
                    // Remaining percentage is no-op (simulates other system activity)
                }
            }
        }

        let elapsed = start_time.elapsed();

        PerformanceMetrics {
            strategy_name: strategy_name.to_string(),
            buffer_slots: config.buffer_slots,
            total_operations: access_sequence.len(),
            cache_hits,
            cache_misses,
            evictions: cache_misses, // Approximation - each miss likely causes eviction
            writes_performed,
            elapsed_nanos: elapsed.as_nanos(),
        }
    }

    /// Generate access sequence based on the access pattern
    fn generate_access_sequence(&self, config: &BenchmarkConfig) -> Vec<u64> {
        match &config.access_pattern {
            AccessPattern::Sequential => (0..config.total_items)
                .cycle()
                .take(config.total_items * 2)
                .map(|i| i as u64)
                .collect(),
            AccessPattern::Random(pattern) => pattern.clone(),
            AccessPattern::Working(pattern) => pattern.clone(),
            AccessPattern::LruWorst => {
                // Generate pattern that's worst case for LRU: access N+1 items repeatedly
                let mut pattern = Vec::new();
                for _ in 0..1000 {
                    for i in 0..=(config.buffer_slots) {
                        pattern.push(i as u64);
                    }
                }
                pattern
            }
        }
    }

    /// Run comprehensive benchmark suite
    pub fn run_benchmark_suite(&self) -> Vec<PerformanceMetrics> {
        let mut results = Vec::new();

        for config in &self.configs {
            for (strategy_name, strategy_fn) in &self.strategies {
                let metrics = self.run_single_benchmark(strategy_name, *strategy_fn, config);
                results.push(metrics);
            }
        }

        results
    }

    /// Generate detailed performance report
    pub fn generate_report(results: Vec<PerformanceMetrics>) -> String {
        let mut report = String::new();
        report.push_str("# Eviction Strategy Performance Analysis\n\n");

        // Group results by configuration
        let mut by_config: std::collections::HashMap<String, Vec<&PerformanceMetrics>> =
            std::collections::HashMap::new();

        for result in &results {
            let config_key = format!(
                "{}_{}_slots",
                result.buffer_slots,
                result.total_operations / result.buffer_slots
            );
            by_config.entry(config_key).or_default().push(result);
        }

        for (config_name, config_results) in by_config {
            report.push_str(&format!("## Configuration: {config_name}\n\n"));
            report.push_str("| Strategy | Hit Rate | Ops/sec | Avg Latency (ns) | Evictions |\n");
            report.push_str("|----------|----------|---------|------------------|----------|\n");

            for result in config_results {
                report.push_str(&format!(
                    "| {} | {:.2}% | {:.0} | {:.2} | {} |\n",
                    result.strategy_name,
                    result.hit_rate() * 100.0,
                    result.operations_per_second(),
                    result.avg_latency_nanos(),
                    result.evictions
                ));
            }
            report.push('\n');
        }

        report
    }
}

/// Criterion benchmark functions
fn benchmark_eviction_strategies(c: &mut Criterion) {
    let benchmark = EvictionBenchmark::new();

    let mut group = c.benchmark_group("eviction_strategies");

    // Test different buffer sizes with fixed workload
    for buffer_size in [2, 4, 8, 16, 32] {
        let config = BenchmarkConfig {
            name: "fixed_workload",
            buffer_slots: buffer_size,
            total_items: 100,
            access_pattern: AccessPattern::Random(
                (0..500).map(|_| fastrand::u64(0..100)).collect(),
            ),
            workload_type: WorkloadType::ReadOnly,
        };

        for (strategy_name, strategy_fn) in &benchmark.strategies {
            group.bench_with_input(
                BenchmarkId::new(*strategy_name, buffer_size),
                &buffer_size,
                |b, _| {
                    b.iter(|| {
                        black_box(benchmark.run_single_benchmark(
                            strategy_name,
                            *strategy_fn,
                            &config,
                        ))
                    })
                },
            );
        }
    }

    group.finish();
}

fn benchmark_slot_allocation_analysis(c: &mut Criterion) {
    let benchmark = EvictionBenchmark::new();

    let mut group = c.benchmark_group("slot_allocation");

    // Test how performance scales with buffer pool size
    let total_items = 1000;
    for buffer_ratio in [0.01, 0.05, 0.1, 0.2, 0.5] {
        let buffer_slots = ((total_items as f64) * buffer_ratio) as usize;
        let config = BenchmarkConfig {
            name: "scaling_test",
            buffer_slots,
            total_items,
            access_pattern: AccessPattern::Random(
                (0..2000)
                    .map(|_| fastrand::u64(0..total_items as u64))
                    .collect(),
            ),
            workload_type: WorkloadType::ReadOnly,
        };

        group.bench_with_input(
            BenchmarkId::new("bottom_evictor", format!("{:.0}%", buffer_ratio * 100.0)),
            &buffer_ratio,
            |b, _| {
                b.iter(|| {
                    black_box(benchmark.run_single_benchmark(
                        "bottom_evictor",
                        bufferpool::bottom_evictor,
                        &config,
                    ))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_eviction_strategies,
    benchmark_slot_allocation_analysis
);
criterion_main!(benches);

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::{
        AccessPattern, BenchmarkConfig, EvictionBenchmark, PerformanceMetrics, WorkloadType,
    };
    #[allow(unused_imports)]
    use bufferpool::bufferpool;

    #[test]
    fn test_benchmark_runs_successfully() {
        let benchmark = EvictionBenchmark::new();
        let config = BenchmarkConfig {
            name: "test_config",
            buffer_slots: 2,
            total_items: 10,
            access_pattern: AccessPattern::Sequential,
            workload_type: WorkloadType::ReadOnly,
        };

        let result =
            benchmark.run_single_benchmark("bottom_evictor", bufferpool::bottom_evictor, &config);

        assert!(result.total_operations > 0);
        assert!(result.elapsed_nanos > 0);
        assert_eq!(result.strategy_name, "bottom_evictor");
        assert_eq!(result.buffer_slots, 2);
    }

    #[test]
    fn test_performance_metrics_calculations() {
        let metrics = PerformanceMetrics {
            strategy_name: "test".to_string(),
            buffer_slots: 4,
            total_operations: 100,
            cache_hits: 80,
            cache_misses: 20,
            evictions: 20,
            writes_performed: 10,
            elapsed_nanos: 1_000_000, // 1ms
        };

        assert!((metrics.hit_rate() - 0.8).abs() < 0.001);
        assert!((metrics.operations_per_second() - 100_000.0).abs() < 1.0);
        assert!((metrics.avg_latency_nanos() - 10_000.0).abs() < 1.0);
    }

    #[test]
    fn test_working_set_pattern_generation() {
        let pattern = EvictionBenchmark::generate_working_set_pattern(100, 20, 1000);
        assert_eq!(pattern.len(), 1000);

        // Most accesses should be in working set (0-19)
        let working_set_accesses = pattern.iter().filter(|&&x| x < 20).count();
        assert!(working_set_accesses > 600); // Should be around 80%
    }

    #[test]
    fn test_benchmark_suite_completeness() {
        let benchmark = EvictionBenchmark::new();
        let results = benchmark.run_benchmark_suite();

        let expected_results = benchmark.configs.len() * benchmark.strategies.len();
        assert_eq!(results.len(), expected_results);

        // Each strategy should be tested
        for (strategy_name, _) in &benchmark.strategies {
            assert!(results.iter().any(|r| r.strategy_name == *strategy_name));
        }
    }
}
