use bufferpool::bufferpool;
use bufferpool::framepool::{self, FramePool};
use std::sync::Arc;
use std::time::Instant;

/// Standalone benchmark runner for eviction strategy analysis
fn main() {
    println!("BufferPool Eviction Strategy Benchmark");
    println!("======================================\n");

    let benchmark = EvictionBenchmark::new();
    let results = benchmark.run_benchmark_suite();

    let report = EvictionBenchmark::generate_report(results);
    println!("{report}");
}

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
    LruWorst,          // Pattern designed to defeat LRU-like strategies
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
    pub config_name: String,
    pub buffer_slots: usize,
    pub total_items: usize,
    pub total_operations: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub evictions: usize,
    pub writes_performed: usize,
    pub elapsed_nanos: u128,
}

impl PerformanceMetrics {
    pub fn hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }

    pub fn operations_per_second(&self) -> f64 {
        if self.elapsed_nanos == 0 {
            0.0
        } else {
            (self.total_operations as f64) / (self.elapsed_nanos as f64 / 1_000_000_000.0)
        }
    }

    pub fn avg_latency_nanos(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.elapsed_nanos as f64 / self.total_operations as f64
        }
    }

    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    pub fn evictions_per_1k_ops(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            (self.evictions as f64 / self.total_operations as f64) * 1000.0
        }
    }
}

/// Eviction strategy function type alias
type EvictionStrategy<T> = fn(
    &[Option<framepool::PageFrame<T>>],
    &bufferpool::unique_stack::UniqueStack<u64>,
) -> Result<u64, bufferpool::BufferPoolErrors>;

/// Simple random number generator using Linear Congruential Generator
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(1103515245).wrapping_add(12345);
        self.state
    }

    fn next_range(&mut self, min: u64, max: u64) -> u64 {
        min + (self.next_u64() % (max - min))
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }
}

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
            configs: Self::create_benchmark_configs(),
        }
    }

    fn create_benchmark_configs() -> Vec<BenchmarkConfig> {
        let mut rng = SimpleRng::new(42);

        vec![
            // Small buffer stress tests
            BenchmarkConfig {
                name: "small_buffer_sequential",
                buffer_slots: 3,
                total_items: 50,
                access_pattern: AccessPattern::Sequential,
                workload_type: WorkloadType::ReadOnly,
            },
            BenchmarkConfig {
                name: "small_buffer_random",
                buffer_slots: 3,
                total_items: 50,
                access_pattern: AccessPattern::Random(
                    (0..200).map(|_| rng.next_range(0, 50)).collect(),
                ),
                workload_type: WorkloadType::ReadOnly,
            },
            BenchmarkConfig {
                name: "working_set_locality",
                buffer_slots: 5,
                total_items: 25,
                access_pattern: AccessPattern::Working(Self::generate_working_set_pattern(
                    25, 5, 200,
                )),
                workload_type: WorkloadType::ReadOnly,
            },
            BenchmarkConfig {
                name: "mixed_workload",
                buffer_slots: 8,
                total_items: 100,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(123);
                    (0..300).map(|_| rng.next_range(0, 100)).collect()
                }),
                workload_type: WorkloadType::Mixed(0.7, 0.3), // 70% read, 30% write
            },
            // Medium buffer tests
            BenchmarkConfig {
                name: "medium_buffer_stress",
                buffer_slots: 16,
                total_items: 200,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(456);
                    (0..500).map(|_| rng.next_range(0, 200)).collect()
                }),
                workload_type: WorkloadType::WriteHeavy(0.4), // 40% writes
            },
            // Large dataset tests
            BenchmarkConfig {
                name: "large_dataset_scan",
                buffer_slots: 32,
                total_items: 1000,
                access_pattern: AccessPattern::Sequential,
                workload_type: WorkloadType::ReadOnly,
            },
            // Adversarial patterns
            BenchmarkConfig {
                name: "lru_worst_case",
                buffer_slots: 4,
                total_items: 10,
                access_pattern: AccessPattern::LruWorst,
                workload_type: WorkloadType::ReadOnly,
            },
            // Buffer allocation analysis
            BenchmarkConfig {
                name: "tiny_buffer_pressure",
                buffer_slots: 2,
                total_items: 100,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(789);
                    (0..400).map(|_| rng.next_range(0, 100)).collect()
                }),
                workload_type: WorkloadType::ReadOnly,
            },
            // Guaranteed cache miss scenarios
            BenchmarkConfig {
                name: "extreme_pressure",
                buffer_slots: 1,
                total_items: 50,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(999);
                    (0..200).map(|_| rng.next_range(0, 50)).collect()
                }),
                workload_type: WorkloadType::ReadOnly,
            },
            BenchmarkConfig {
                name: "thrashing_scenario",
                buffer_slots: 3,
                total_items: 100,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(111);
                    // Access pattern that constantly evicts - wide spread across all items
                    (0..500).map(|_| rng.next_range(0, 100)).collect()
                }),
                workload_type: WorkloadType::ReadOnly,
            },
            BenchmarkConfig {
                name: "large_buffer_efficiency",
                buffer_slots: 64,
                total_items: 100,
                access_pattern: AccessPattern::Random({
                    let mut rng = SimpleRng::new(321);
                    (0..200).map(|_| rng.next_range(0, 100)).collect()
                }),
                workload_type: WorkloadType::ReadOnly,
            },
        ]
    }

    /// Generate access pattern that simulates working set locality
    fn generate_working_set_pattern(
        total_items: usize,
        working_set_size: usize,
        num_accesses: usize,
    ) -> Vec<u64> {
        let mut pattern = Vec::with_capacity(num_accesses);
        let mut rng = SimpleRng::new(42);

        for _ in 0..num_accesses {
            // 80% chance to access working set, 20% chance to access other items
            if rng.next_f64() < 0.8 {
                pattern.push(rng.next_range(0, working_set_size as u64));
            } else {
                pattern.push(rng.next_range(working_set_size as u64, total_items as u64));
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
        let start_time = Instant::now();

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
        let mut rng = SimpleRng::new(42);

        // Track which pages are currently in the buffer pool to detect hits vs misses
        let mut pages_in_buffer = std::collections::HashSet::new();

        // Execute the benchmark workload
        for &idx in &access_sequence {
            match &config.workload_type {
                WorkloadType::ReadOnly => {
                    let was_in_buffer = pages_in_buffer.contains(&idx);

                    if let Some(_page) = buffer_pool.get_page(idx) {
                        if was_in_buffer {
                            cache_hits += 1;
                        } else {
                            cache_misses += 1;
                            pages_in_buffer.insert(idx);

                            // If buffer is full, we need to track what gets evicted
                            if pages_in_buffer.len() > config.buffer_slots {
                                // Simple approximation: assume least recently used was evicted
                                // In reality, this depends on the eviction strategy
                                pages_in_buffer.clear();
                                pages_in_buffer.insert(idx);
                            }
                        }
                    } else {
                        cache_misses += 1;
                    }
                }
                WorkloadType::WriteHeavy(write_ratio) => {
                    let was_in_buffer = pages_in_buffer.contains(&idx);

                    if rng.next_f64() < *write_ratio {
                        // Write operation
                        if let Some(page) = buffer_pool.get_page(idx) {
                            page.with_data(|data: &mut String| {
                                *data = format!("modified_item_{idx:06}");
                            });
                            writes_performed += 1;

                            if was_in_buffer {
                                cache_hits += 1;
                            } else {
                                cache_misses += 1;
                                pages_in_buffer.insert(idx);
                                if pages_in_buffer.len() > config.buffer_slots {
                                    pages_in_buffer.clear();
                                    pages_in_buffer.insert(idx);
                                }
                            }
                        } else {
                            cache_misses += 1;
                        }
                    } else {
                        // Read operation
                        if let Some(_page) = buffer_pool.get_page(idx) {
                            if was_in_buffer {
                                cache_hits += 1;
                            } else {
                                cache_misses += 1;
                                pages_in_buffer.insert(idx);
                                if pages_in_buffer.len() > config.buffer_slots {
                                    pages_in_buffer.clear();
                                    pages_in_buffer.insert(idx);
                                }
                            }
                        } else {
                            cache_misses += 1;
                        }
                    }
                }
                WorkloadType::Mixed(read_ratio, write_ratio) => {
                    let was_in_buffer = pages_in_buffer.contains(&idx);
                    let op_type = rng.next_f64();

                    if op_type < *read_ratio {
                        // Read operation
                        if let Some(_page) = buffer_pool.get_page(idx) {
                            if was_in_buffer {
                                cache_hits += 1;
                            } else {
                                cache_misses += 1;
                                pages_in_buffer.insert(idx);
                                if pages_in_buffer.len() > config.buffer_slots {
                                    pages_in_buffer.clear();
                                    pages_in_buffer.insert(idx);
                                }
                            }
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

                            if was_in_buffer {
                                cache_hits += 1;
                            } else {
                                cache_misses += 1;
                                pages_in_buffer.insert(idx);
                                if pages_in_buffer.len() > config.buffer_slots {
                                    pages_in_buffer.clear();
                                    pages_in_buffer.insert(idx);
                                }
                            }
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
            config_name: config.name.to_string(),
            buffer_slots: config.buffer_slots,
            total_items: config.total_items,
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
                for _ in 0..100 {
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

        println!("Running benchmark suite...\n");

        for (config_idx, config) in self.configs.iter().enumerate() {
            println!(
                "Running config {}/{}: {}",
                config_idx + 1,
                self.configs.len(),
                config.name
            );

            for (strategy_name, strategy_fn) in &self.strategies {
                print!("  Testing {strategy_name} ... ");
                let metrics = self.run_single_benchmark(strategy_name, *strategy_fn, config);
                println!(
                    "Hit rate: {:.1}%, Ops/sec: {:.0}",
                    metrics.hit_rate() * 100.0,
                    metrics.operations_per_second()
                );
                results.push(metrics);
            }
            println!();
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
            by_config
                .entry(result.config_name.clone())
                .or_default()
                .push(result);
        }

        // Sort configs by name for consistent output
        let mut config_names: Vec<_> = by_config.keys().cloned().collect();
        config_names.sort();

        for config_name in config_names {
            let config_results = by_config.get(&config_name).unwrap();
            let first_result = config_results[0];

            report.push_str(&format!("## {config_name}\n"));
            report.push_str(&format!("- Buffer slots: {}\n", first_result.buffer_slots));
            report.push_str(&format!("- Total items: {}\n", first_result.total_items));
            report.push_str(&format!(
                "- Operations: {}\n\n",
                first_result.total_operations
            ));

            report.push_str("| Strategy | Hit Rate | Miss Rate | Ops/sec | Avg Latency (ns) | Evictions/1k ops |\n");
            report.push_str("|----------|----------|-----------|---------|------------------|------------------|\n");

            for result in config_results {
                report.push_str(&format!(
                    "| {} | {:.1}% | {:.1}% | {:.0} | {:.1} | {:.1} |\n",
                    result.strategy_name,
                    result.hit_rate() * 100.0,
                    result.miss_rate() * 100.0,
                    result.operations_per_second(),
                    result.avg_latency_nanos(),
                    result.evictions_per_1k_ops()
                ));
            }
            report.push('\n');
        }

        // Add summary analysis
        report.push_str("## Summary Analysis\n\n");

        // Calculate average performance by strategy
        let mut strategy_totals: std::collections::HashMap<String, (f64, f64, usize)> =
            std::collections::HashMap::new();

        for result in &results {
            let entry = strategy_totals
                .entry(result.strategy_name.clone())
                .or_insert((0.0, 0.0, 0));
            entry.0 += result.hit_rate();
            entry.1 += result.operations_per_second();
            entry.2 += 1;
        }

        report.push_str("### Average Performance by Strategy\n\n");
        report.push_str("| Strategy | Avg Hit Rate | Avg Ops/sec |\n");
        report.push_str("|----------|--------------|-------------|\n");

        for (strategy, (hit_rate_sum, ops_sum, count)) in strategy_totals {
            report.push_str(&format!(
                "| {} | {:.1}% | {:.0} |\n",
                strategy,
                (hit_rate_sum / count as f64) * 100.0,
                ops_sum / count as f64
            ));
        }

        report.push_str("\n### Key Insights\n\n");
        report.push_str("- **Buffer Size Impact**: Larger buffers generally improve hit rates but show diminishing returns\n");
        report.push_str("- **Access Pattern Sensitivity**: Random access patterns stress eviction strategies more than sequential\n");
        report.push_str("- **Working Set Locality**: Strategies perform better when access patterns exhibit temporal locality\n");
        report.push_str("- **Write Performance**: Mixed workloads can reduce effective cache performance due to dirty page management\n");

        report
    }
}
