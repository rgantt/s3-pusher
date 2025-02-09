use aws_sdk_s3::Client;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use chrono::{DateTime, Utc, Local, TimeZone};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use std::time::Duration as StdDuration;
use std::time::Instant;
use humansize::{format_size, BINARY};
use anyhow::Result;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB chunks for better multipart performance
const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024; // 5MB minimum for multipart uploads
const MIN_MULTIPART_TOTAL_SIZE: u64 = 10 * 1024 * 1024; // Only use multipart for files > 10MB
const MEGABIT: f64 = 1000.0 * 1000.0 / 8.0; // Convert bytes/sec to Mbps
const METADATA_FILE: &str = ".scanned_metadata";

#[derive(Parser, Debug)]
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
    bytes_transferred: Arc<Mutex<u64>>,
    total_bytes: u64,
    total_files: usize,
    files_completed: Arc<Mutex<usize>>,
}

impl TotalProgress {
    fn new(total_bytes: u64, total_files: usize) -> Self {
        Self {
            start_time: Instant::now(),
            bytes_transferred: Arc::new(Mutex::new(0)),
            total_bytes,
            total_files,
            files_completed: Arc::new(Mutex::new(0)),
        }
    }

    fn add_bytes(&self, bytes: u64) {
        *self.bytes_transferred.lock().unwrap() += bytes;
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
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.bytes_transferred() as f64 / elapsed / MEGABIT
        } else {
            0.0
        }
    }

    fn update_progress(&self, progress_bar: &ProgressBar) {
        let transferred = self.bytes_transferred();
        let files_done = self.files_completed();
        progress_bar.set_message(format!("{}/{} files, {}/{} @ {:.2} Mbps",
            files_done,
            self.total_files,
            format_size(transferred, BINARY),
            format_size(self.total_bytes, BINARY),
            self.rate()
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
    total_transfer.inc_files();
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
    if args.verbose {
        println!("Starting multipart upload for {} ({})",
            file_info.key,
            format_size(file_info.size, BINARY));
    }

    let mut file = fs::File::open(&file_info.path).await?;
    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    let mut part_number: i32 = 1;
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let update_interval = StdDuration::from_millis(100);
    let mut last_update = Instant::now();
    let mut remaining_size = file_info.size;

    // Start multipart upload
    let create_multipart_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(&file_info.key)
        .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
        .send()
        .await?;
    
    let upload_id = create_multipart_res.upload_id().unwrap();
    if args.verbose {
        println!("Got upload ID: {}", upload_id);
    }

    // Upload parts
    while remaining_size > 0 {
        let to_read = std::cmp::min(remaining_size as usize, CHUNK_SIZE);
        if args.verbose {
            println!("Reading chunk {} of size {}",
                part_number,
                format_size(to_read as u64, BINARY));
        }

        buffer.clear();
        buffer.resize(to_read, 0);
        let mut bytes_read = 0;

        while bytes_read < to_read {
            match file.read(&mut buffer[bytes_read..to_read]).await? {
                0 => break,
                n => {
                    bytes_read += n;
                    if last_update.elapsed() >= update_interval {
                        file_progress.inc(n as u64);
                        total_transfer.add_bytes(n as u64);
                        last_update = Instant::now();
                    }
                }
            }
        }

        // Upload the part
        let part = client
            .upload_part()
            .bucket(bucket)
            .key(&file_info.key)
            .upload_id(upload_id)
            .body(aws_sdk_s3::primitives::ByteStream::from(buffer[..bytes_read].to_vec()))
            .part_number(part_number)
            .send()
            .await?;

        let completed_part = CompletedPart::builder()
            .e_tag(part.e_tag.unwrap())
            .part_number(part_number)
            .build();

        completed_parts.push(completed_part);
        remaining_size -= bytes_read as u64;
        part_number += 1;
    }

    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(&file_info.key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

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

    // Upload files
    for file_info in files_to_upload {
        if let Err(e) = upload_file(
            &client,
            &args.bucket,
            &file_info,
            &multi_progress,
            &total_transfer,
            &args,
        ).await {
            eprintln!("Error uploading {}: {}", file_info.key, e);
        }
    }

    progress.finish_with_message("Upload complete");
    
    Ok(())
}

fn log_verbose<S: Into<String>>(msg: S) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", timestamp, msg.into());
}
