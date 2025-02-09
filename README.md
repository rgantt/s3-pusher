# s3-pusher

A high-performance CLI tool for efficiently uploading directories to Amazon S3 with smart file synchronization and progress tracking.

## Features

- 🚀 Fast parallel uploads using multipart upload for large files
- 💡 Smart synchronization - only uploads new or modified files
- 📊 Real-time progress tracking with transfer speeds
- 🔄 Caching of metadata to speed up subsequent uploads
- 📁 Preserves directory structure in S3
- 🔍 Verbose mode for detailed operation logging
- 🎯 Memory-efficient buffer pool for optimal performance
- 🔄 Configurable parallel upload settings

## Installation

### Prerequisites

- Rust toolchain (1.56 or later)
- AWS credentials configured (either through environment variables, AWS CLI, or IAM role)

### Building from Source

```bash
cargo build --release
```

The binary will be available at `target/release/s3-pusher`

## Usage

```bash
s3-pusher --bucket <BUCKET_NAME> --directory <LOCAL_DIR> [OPTIONS]
```

### Arguments

Required:
- `--bucket`, `-b`: The name of the S3 bucket to upload to
- `--directory`, `-d`: The local directory to upload

Optional:
- `--verbose`, `-v`: Enable verbose logging
- `--cached-metadata`: Path to cached metadata file (speeds up synchronization)
- `--parallel-uploads`: Number of files to upload in parallel (default: 2)
- `--concurrent-parts`: Number of concurrent part uploads per file (default: 10)

### Examples

Basic usage:
```bash
s3-pusher --bucket my-bucket --directory ./data
```

With verbose logging:
```bash
s3-pusher --bucket my-bucket --directory ./data --verbose
```

Using cached metadata:
```bash
s3-pusher --bucket my-bucket --directory ./data --cached-metadata .metadata.json
```

Customizing parallel uploads:
```bash
s3-pusher --bucket my-bucket --directory ./data --parallel-uploads 4 --concurrent-parts 15
```

## Performance

- Uses 64MB chunks for multipart uploads (optimized for modern network speeds)
- Automatically switches to multipart upload for files larger than 10MB
- Memory-efficient buffer pool to reduce allocation overhead
- Configurable parallel processing:
  - Control number of concurrent file uploads
  - Adjust concurrent part uploads per file
- Real-time progress bars showing:
  - Overall progress (files completed, total bytes transferred)
  - Current transfer speed in MiB/s
  - Individual file progress
  - ETA for completion

## Implementation Details

- Written in Rust for maximum performance and reliability
- Uses `aws-sdk-s3` for S3 operations
- Advanced multipart upload features:
  - Buffer pool for memory efficiency
  - Configurable concurrency settings
  - Automatic part size optimization
- Maintains a local metadata cache to optimize synchronization
- Progress tracking using `indicatif` for a great CLI experience

## AWS Credentials

The tool uses the standard AWS credential chain. Make sure you have valid AWS credentials configured through one of these methods:

1. Environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (if running on AWS infrastructure)

## License

This project is open source and available under the MIT License.
