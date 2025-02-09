use aws_sdk_s3::Client;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use chrono::{DateTime, Utc, Local, TimeZone};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Semaphore;
use std::time::Duration as StdDuration;
use std::time::Instant;
use humansize::{format_size, BINARY};
use anyhow::Result;

const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB chunks for better multipart performance
const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024; // 5MB minimum for multipart uploads
const MIN_MULTIPART_TOTAL_SIZE: u64 = 10 * 1024 * 1024; // Only use multipart for files > 10MB
const MIB: f64 = 1024.0 * 1024.0; // bytes to MiB conversion
const RATE_WINDOW: f64 = 1.0; // Calculate rates over a 1-second window
const METADATA_FILE: &str = ".scanned_metadata";
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB minimum part size

// Buffer pool for reusing allocated buffers in multipart uploads
struct BufferPool {
    buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,
    buffer_size: usize,
}

impl BufferPool {
    fn new(pool_size: usize, buffer_size: usize) -> Self {
        let mut buffers = VecDeque::with_capacity(pool_size);
        for _ in 0..pool_size {
            buffers.push_back(vec![0; buffer_size]);
        }
        BufferPool {
            buffers: Arc::new(Mutex::new(buffers)),
            buffer_size,
        }
    }

    async fn acquire(&self) -> Vec<u8> {
        let mut buffers = self.buffers.lock().unwrap();
        buffers.pop_front().unwrap_or_else(|| vec![0; self.buffer_size])
    }

    fn release(&self, buffer: Vec<u8>) {
        if buffer.capacity() == self.buffer_size {
            let mut buffers = self.buffers.lock().unwrap();
            buffers.push_back(buffer);
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    bucket: String,

    #[arg(short, long)]
    directory: PathBuf,

    #[arg(short, long)]
    verbose: bool,

    #[arg(long)]
    cached_metadata: Option<PathBuf>,

    #[arg(long, default_value = "2", help = "Number of files to upload in parallel")]
    parallel_uploads: usize,

    #[arg(long, default_value = "10", help = "Number of concurrent part uploads per file")]
    concurrent_parts: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileInfo {
    path: PathBuf,
    key: String,
    modified: DateTime<Utc>,
    size: u64,
}

#[derive(Debug)]
struct TotalProgress {
    start_time: Instant,
    last_update_time: Arc<Mutex<Instant>>,
    last_bytes: Arc<Mutex<u64>>,
    bytes_transferred: Arc<Mutex<u64>>,
    total_bytes: u64,
    total_files: usize,
    files_completed: Arc<Mutex<usize>>,
    current_rate: Arc<Mutex<f64>>,
}

impl TotalProgress {
    fn new(total_bytes: u64, total_files: usize) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_update_time: Arc::new(Mutex::new(now)),
            last_bytes: Arc::new(Mutex::new(0)),
            bytes_transferred: Arc::new(Mutex::new(0)),
            total_bytes,
            total_files,
            files_completed: Arc::new(Mutex::new(0)),
            current_rate: Arc::new(Mutex::new(0.0)),
        }
    }

    fn add_bytes(&self, bytes: u64) {
        let mut bytes_transferred = self.bytes_transferred.lock().unwrap();
        *bytes_transferred += bytes;
        
        // Update rate calculation
        let now = Instant::now();
        let mut last_update = self.last_update_time.lock().unwrap();
        let mut last_bytes = self.last_bytes.lock().unwrap();
        let mut current_rate = self.current_rate.lock().unwrap();
        
        let elapsed = now.duration_since(*last_update).as_secs_f64();
        if elapsed >= RATE_WINDOW {
            let rate = (*bytes_transferred - *last_bytes) as f64 / elapsed / MIB;
            *current_rate = rate;
            *last_bytes = *bytes_transferred;
            *last_update = now;
        }
    }

    fn complete_file(&self, file_size: u64) {
        let mut bytes_transferred = self.bytes_transferred.lock().unwrap();
        // Calculate how many bytes we need to add to reach the file size
        // This ensures we don't miss any bytes due to chunked reading
        let remaining_bytes = file_size.saturating_sub(*bytes_transferred % file_size);
        *bytes_transferred += remaining_bytes;
        
        self.inc_files();
    }

    fn inc_files(&self) {
        *self.files_completed.lock().unwrap() += 1;
    }

    fn bytes_transferred(&self) -> u64 {
        *self.bytes_transferred.lock().unwrap()
    }

    fn files_completed(&self) -> usize {
        *self.files_completed.lock().unwrap()
    }

    fn rate(&self) -> f64 {
        *self.current_rate.lock().unwrap()
    }

    fn update_progress(&self, progress_bar: &ProgressBar) {
        let transferred = self.bytes_transferred();
        let files_done = self.files_completed();
        let current_rate = self.rate();
        let avg_rate = if self.start_time.elapsed().as_secs_f64() > 0.0 {
            transferred as f64 / self.start_time.elapsed().as_secs_f64() / MIB
        } else {
            0.0
        };
        
        // Set the actual progress position
        progress_bar.set_position(transferred);
        
        progress_bar.set_message(format!("{}/{} files, {}/{} @ {:.2} MiB/s (avg: {:.2} MiB/s)",
            files_done,
            self.total_files,
            format_size(transferred, BINARY),
            format_size(self.total_bytes, BINARY),
            current_rate,
            avg_rate
        ));
    }
}

async fn get_s3_client() -> Result<Client> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    Ok(Client::new(&config))
}

async fn scan_directory(base_dir: &Path) -> Result<Vec<FileInfo>> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.blue} {msg}")
        .unwrap());
    spinner.set_message(format!("Scanning {}", base_dir.display()));
    spinner.enable_steady_tick(StdDuration::from_millis(100));
    
    let mut files = Vec::new();
    let base_dir_str = base_dir.to_string_lossy();
    let mut entries = fs::read_dir(base_dir).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.file_name().unwrap_or_default() == METADATA_FILE {
            continue;
        }

        let metadata = fs::metadata(&path).await?;
        if metadata.is_file() {
            let modified = DateTime::<Utc>::from(metadata.modified()?);
            let key = if base_dir_str == "." {
                path.to_string_lossy().into_owned()
            } else {
                path.strip_prefix(base_dir)?
                    .to_string_lossy()
                    .into_owned()
            };
            
            files.push(FileInfo {
                path: path.clone(),
                key,
                modified,
                size: metadata.len(),
            });
            
            spinner.set_message(format!("Found {} files", files.len()));
        } else if metadata.is_dir() {
            let mut subdir_files = Box::pin(scan_directory(&path)).await?;
            files.append(&mut subdir_files);
        }
    }
    
    spinner.finish_and_clear();
    Ok(files)
}

async fn get_s3_metadata(client: &Client, bucket: &str, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
    let mut files = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut list_objects = client.list_objects_v2()
            .bucket(bucket);
        
        if let Some(prefix) = prefix {
            list_objects = list_objects.prefix(prefix);
        }
        
        if let Some(token) = continuation_token {
            list_objects = list_objects.continuation_token(token);
        }

        let response = list_objects.send().await?;
        let next_token = response.next_continuation_token().map(|s| s.to_string());
        
        if let Some(contents) = response.contents {
            for object in contents {
                if let (Some(key), Some(modified), Some(size)) = (object.key(), object.last_modified(), object.size()) {
                    // Convert AWS DateTime to chrono DateTime<Utc>
                    let timestamp = modified.as_secs_f64();
                    let secs = timestamp.floor() as i64;
                    let nsecs = ((timestamp - secs as f64) * 1_000_000_000.0) as u32;
                    if let Some(datetime) = Utc.timestamp_opt(secs, nsecs).single() {
                        files.push(FileInfo {
                            key: key.to_string(),
                            path: PathBuf::new(), // S3 files don't have local paths
                            size: size as u64,
                            modified: datetime,
                        });
                    }
                }
            }
        }

        match next_token {
            Some(token) => continuation_token = Some(token),
            None => break,
        }
    }

    Ok(files)
}

async fn reconcile_metadata(local_files: &[FileInfo], s3_files: &[FileInfo], args: &Args) -> Vec<FileInfo> {
    let mut to_upload = Vec::new();

    for file in local_files {
        let should_upload = match s3_files.iter().find(|f| f.key == file.key) {
            Some(s3_file) => {
                // Add 1 second buffer to handle potential timestamp precision differences
                file.modified > s3_file.modified + chrono::Duration::seconds(1)
            },
            None => true,
        };

        if should_upload {
            if args.verbose {
                println!("Will upload {} (local: {:?}, s3: {:?})", 
                    file.key, 
                    file.modified,
                    s3_files.iter().find(|f| f.key == file.key).map(|t| t.modified.to_string()).unwrap_or_else(|| "not in S3".to_string())
                );
            }
            to_upload.push(file.clone());
        } else if args.verbose {
            println!("Skipping {} (already up to date)", file.key);
        }
    }

    to_upload
}

async fn upload_file(
    client: &Client,
    bucket: &str,
    file_info: &FileInfo,
    multi_progress: &MultiProgress,
    total_transfer: &Arc<TotalProgress>,
    args: &Args,
) -> Result<()> {
    let file_progress = multi_progress.add(ProgressBar::new(file_info.size));
    file_progress.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) - {msg}")
        .unwrap()
        .progress_chars("=>-"));
    file_progress.set_message(file_info.key.to_string());
    file_progress.enable_steady_tick(StdDuration::from_millis(100));

    // Only use multipart upload for files larger than 10MB
    if file_info.size >= MIN_MULTIPART_TOTAL_SIZE {
        upload_multipart_file(client, bucket, file_info, &file_progress, total_transfer, multi_progress, args).await?;
    } else {
        upload_small_file(client, bucket, file_info, &file_progress, total_transfer, multi_progress, args).await?;
    }

    file_progress.finish_and_clear();
    total_transfer.complete_file(file_info.size);
    Ok(())
}

async fn upload_multipart_file(
    client: &Client,
    bucket: &str,
    file_info: &FileInfo,
    file_progress: &ProgressBar,
    total_transfer: &Arc<TotalProgress>,
    _multi_progress: &MultiProgress,
    args: &Args,
) -> Result<()> {
    // Create a buffer pool with size equal to concurrent_parts
    let buffer_pool = Arc::new(BufferPool::new(args.concurrent_parts, CHUNK_SIZE));
    
    // Initialize multipart upload
    let create_multipart_resp = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(&file_info.key)
        .send()
        .await?;

    let upload_id = create_multipart_resp.upload_id().ok_or_else(|| {
        anyhow::anyhow!("Failed to get upload ID from create_multipart_upload response")
    })?;

    let completed_parts = Arc::new(TokioMutex::new(Vec::new()));
    let last_update = Arc::new(TokioMutex::new(Instant::now()));
    let update_interval = StdDuration::from_millis(100);

    let file_size = file_info.size as usize;
    let num_parts = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    let semaphore = Arc::new(Semaphore::new(args.concurrent_parts));
    let mut upload_tasks = Vec::new();

    for part_number in 1..=num_parts {
        let start_pos = (part_number - 1) * CHUNK_SIZE;
        let this_part_size = if part_number == num_parts {
            file_size - start_pos
        } else {
            CHUNK_SIZE
        };

        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = file_info.key.clone();
        let upload_id = upload_id.to_string();
        let completed_parts = completed_parts.clone();
        let file_progress = file_progress.clone();
        let total_transfer = total_transfer.clone();
        let last_update = last_update.clone();
        let verbose = args.verbose;
        let buffer_pool = buffer_pool.clone();
        let file_path = file_info.path.clone();

        let task = tokio::spawn(async move {
            // Open a new file handle for this part
            let mut file = fs::File::open(&file_path).await?;
            file.seek(std::io::SeekFrom::Start(start_pos as u64)).await?;
            
            // Get a buffer from the pool
            let mut buffer = buffer_pool.acquire().await;
            
            let mut bytes_read = 0;
            while bytes_read < this_part_size {
                match file.read(&mut buffer[bytes_read..this_part_size]).await? {
                    0 => break,
                    n => {
                        bytes_read += n;
                        let mut last_update = last_update.lock().await;
                        if last_update.elapsed() >= update_interval {
                            file_progress.inc(n as u64);
                            total_transfer.add_bytes(n as u64);
                            *last_update = Instant::now();
                        }
                    }
                }
            }

            if verbose {
                println!("Uploading part {} ({} bytes)", part_number, bytes_read);
            }

            let part = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .body(aws_sdk_s3::primitives::ByteStream::from(buffer[..bytes_read].to_vec()))
                .part_number(part_number as i32)
                .send()
                .await?;

            // Return the buffer to the pool
            buffer_pool.release(buffer);

            let completed_part = CompletedPart::builder()
                .e_tag(part.e_tag.unwrap())
                .part_number(part_number as i32)
                .build();

            let mut completed_parts = completed_parts.lock().await;
            completed_parts.push(completed_part);

            drop(permit);
            Ok::<_, anyhow::Error>(())
        });

        upload_tasks.push(task);
    }

    // Wait for all upload tasks to complete
    for task in upload_tasks {
        task.await??;
    }

    // Sort completed parts by part number to ensure correct order
    let mut completed_parts = completed_parts.lock().await;
    completed_parts.sort_by_key(|part| part.part_number());
    
    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts.to_vec()))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(&file_info.key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    // Ensure total progress is accurate
    total_transfer.complete_file(file_info.size);

    Ok(())
}

async fn upload_small_file(
    client: &Client,
    bucket: &str,
    file_info: &FileInfo,
    file_progress: &ProgressBar,
    total_transfer: &Arc<TotalProgress>,
    _multi_progress: &MultiProgress,
    args: &Args,
) -> Result<()> {
    if args.verbose {
        println!("Starting small file upload for {} ({})",
            file_info.key,
            format_size(file_info.size, BINARY));
    }

    let mut file = fs::File::open(&file_info.path).await?;
    let mut buffer = Vec::with_capacity(file_info.size as usize);
    let update_interval = StdDuration::from_millis(100);
    let mut last_update = Instant::now();

    loop {
        let mut chunk = vec![0; CHUNK_SIZE];
        match file.read(&mut chunk).await? {
            0 => break,
            n => {
                buffer.extend_from_slice(&chunk[..n]);
                if last_update.elapsed() >= update_interval {
                    file_progress.inc(n as u64);
                    total_transfer.add_bytes(n as u64);
                    last_update = Instant::now();
                }
            }
        }
    }

    client
        .put_object()
        .bucket(bucket)
        .key(&file_info.key)
        .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
        .body(aws_sdk_s3::primitives::ByteStream::from(buffer))
        .send()
        .await?;

    Ok(())
}

async fn load_cached_metadata(path: &Path) -> Result<Option<Vec<FileInfo>>> {
    if path.exists() {
        let content = fs::read_to_string(path).await?;
        let files: Vec<FileInfo> = serde_json::from_str(&content)?;
        if files.is_empty() {
            println!("Found empty metadata file, will perform a fresh scan");
            Ok(None)
        } else {
            println!("Loaded {} files from metadata cache", files.len());
            Ok(Some(files))
        }
    } else {
        println!("No metadata cache found at {}, will perform a fresh scan", path.display());
        Ok(None)
    }
}

async fn save_metadata_cache(path: &Path, files: &[FileInfo]) -> Result<()> {
    println!("Saving metadata cache to {}", path.display());
    let metadata_json = serde_json::to_string_pretty(files)?;
    fs::write(path, metadata_json).await?;
    Ok(())
}

async fn delete_s3_object(
    client: &Client,
    bucket: &str,
    key: &str,
    verbose: bool,
) -> Result<()> {
    if verbose {
        println!("Deleting from S3: {}", key);
    }
    
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = get_s3_client().await?;
    
    // Canonicalize the directory path
    let directory = fs::canonicalize(&args.directory).await?;
    
    if args.verbose {
        println!("Scanning directory: {}", directory.display());
        println!("Target S3 bucket: {}", args.bucket);
    }

    // Get S3 metadata first
    println!("Fetching S3 metadata...");
    let s3_files = get_s3_metadata(&client, &args.bucket, None).await?;
    println!("Found {} files in S3", s3_files.len());

    // Load cached metadata or scan directory
    let files = if let Some(cached_path) = args.cached_metadata.as_ref() {
        match load_cached_metadata(cached_path).await? {
            Some(files) => files,
            None => {
                let files = scan_directory(&directory).await?;
                save_metadata_cache(cached_path, &files).await?;
                files
            }
        }
    } else {
        println!("No cached metadata file specified, performing fresh scan...");
        scan_directory(&directory).await?
    };

    // Calculate total size of all files and files to upload
    let total_size: u64 = files.iter().map(|f| f.size).sum();
    let files_to_upload: Vec<_> = files.into_iter()
        .filter(|file| {
            if let Some(s3_file) = s3_files.iter().find(|f| f.key == file.key) {
                file.size != s3_file.size
            } else {
                true
            }
        })
        .collect();
    let upload_size: u64 = files_to_upload.iter().map(|f| f.size).sum();

    println!("Total size of all files: {}", format_size(total_size, BINARY));
    println!("Found {} files ({}) to upload", 
        files_to_upload.len(),
        format_size(upload_size, BINARY));
    
    // Set up progress bars only after we have all the metadata
    let multi_progress = MultiProgress::new();
    
    // Combined progress bar
    let progress = multi_progress.add(ProgressBar::new(upload_size));
    progress.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {msg}")
        .unwrap()
        .progress_chars("=>-"));
    progress.enable_steady_tick(StdDuration::from_millis(100));
    
    // Set up total transfer tracking
    let total_transfer = Arc::new(TotalProgress::new(upload_size, files_to_upload.len()));
    
    // Start progress update task
    let progress_clone = progress.clone();
    let total_transfer_clone = total_transfer.clone();
    tokio::spawn(async move {
        loop {
            total_transfer_clone.update_progress(&progress_clone);
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }
    });

    // Create a shared queue for all workers
    let queue = Arc::new(TokioMutex::new(VecDeque::from_iter(
        files_to_upload.into_iter().map(|f| Arc::new(f))
    )));

    // Spawn parallel upload workers
    let mut worker_handles = vec![];
    for worker_id in 0..args.parallel_uploads {
        let client = client.clone();
        let bucket = args.bucket.clone();
        let multi_progress = multi_progress.clone();
        let total_transfer = total_transfer.clone();
        let args = args.clone();
        let error_pb = multi_progress.add(ProgressBar::hidden());
        let queue = queue.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Try to get next file from queue
                let file_info = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };

                match file_info {
                    Some(file_info) => {
                        if let Err(e) = upload_file(
                            &client,
                            &bucket,
                            &file_info,
                            &multi_progress,
                            &total_transfer,
                            &args,
                        ).await {
                            error_pb.println(format!("Error uploading {}: {}", file_info.key, e));
                        }
                    },
                    None => break, // No more files to process
                }
            }
            error_pb.finish_and_clear();
        });
        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    progress.finish_with_message("Upload complete");
    
    Ok(())
}

fn log_verbose<S: Into<String>>(msg: S) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", timestamp, msg.into());
}
