# s3-pusher

A high-performance CLI tool for efficiently uploading directories to Amazon S3 with smart file synchronization and progress tracking.

## Features

- 🚀 Fast parallel uploads using multipart upload for large files
- 💡 Smart synchronization - only uploads new or modified files
- 📊 Real-time progress tracking with transfer speeds
- 🔄 Caching of metadata to speed up subsequent uploads
- 📁 Preserves directory structure in S3
- 🔍 Verbose mode for detailed operation logging

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

- `--bucket`, `-b`: The name of the S3 bucket to upload to (required)
- `--directory`, `-d`: The local directory to upload (required)
- `--verbose`, `-v`: Enable verbose logging
- `--cached-metadata`: Path to cached metadata file (optional, speeds up synchronization)

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

## Performance

- Uses 8MB chunks for multipart uploads
- Automatically switches to multipart upload for files larger than 10MB
- Parallel processing of uploads for improved throughput
- Real-time progress bars showing:
  - Overall progress (files completed, total bytes transferred)
  - Current transfer speed in Mbps
  - Individual file progress
  - ETA for completion

## Implementation Details

- Written in Rust for maximum performance and reliability
- Uses `aws-sdk-s3` for S3 operations
- Implements efficient multipart uploads for large files
- Maintains a local metadata cache to optimize synchronization
- Progress tracking using `indicatif` for a great CLI experience

## Dependencies

- aws-config (1.0.1)
- aws-sdk-s3 (1.4.0)
- tokio (1.28)
- clap (4.3)
- indicatif (0.17)
- And other supporting libraries (see Cargo.toml for full list)

## AWS Credentials

The tool uses the standard AWS credential chain. Make sure you have valid AWS credentials configured through one of these methods:

1. Environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (if running on AWS infrastructure)

## License

This project is open source and available under the MIT License.
