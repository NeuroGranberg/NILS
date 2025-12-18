export interface SystemResources {
  cpu_count: number;
  memory_total: number;
  memory_available: number;
  disk_read_bytes_per_sec: number;
  disk_write_bytes_per_sec: number;
  recommended_processes: number;
  recommended_workers: number;
  recommended_queue_depth: number;
  recommended_batch_size: number;
  recommended_adaptive_min_batch: number;
  recommended_adaptive_max_batch: number;
  recommended_series_workers_per_subject: number;
  recommended_db_writer_pool: number;
  safe_instance_batch_rows: number;
  max_workers_cap: number;
  max_batch_cap: number;
  max_queue_cap: number;
  max_adaptive_batch_cap: number;
  max_db_writer_pool_cap: number;
}
