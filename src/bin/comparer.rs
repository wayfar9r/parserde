use std::{fs::File, path::PathBuf};

use clap::{Parser, ValueEnum};

use std::process::ExitCode;

use parserde::build_reader;

use log::{error, info, warn};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long)]
    file1: PathBuf,
    #[arg(long)]
    file1_format: InputFormat,
    #[arg(long)]
    file2: PathBuf,
    #[arg(short, long)]
    file2_format: InputFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum InputFormat {
    Csv,
    Txt,
    Bin,
}

impl From<InputFormat> for &str {
    fn from(value: InputFormat) -> Self {
        match value {
            InputFormat::Csv => "csv",
            InputFormat::Bin => "bin",
            InputFormat::Txt => "txt",
        }
    }
}

fn main() -> ExitCode {
    env_logger::init();
    let args = Args::parse();
    let file1 = match File::open(args.file1) {
        Ok(f) => f,
        Err(e) => {
            error!("failed to open an input file1. {}", e);
            return ExitCode::FAILURE;
        }
    };
    let file2 = match File::open(args.file2) {
        Ok(f) => f,
        Err(e) => {
            error!("failed to open an input file2. {}", e);
            return ExitCode::FAILURE;
        }
    };
    let mut reader1 = match build_reader(file1, args.file1_format.into()) {
        Ok(r) => r,
        Err(e) => {
            error!("failed to create reader from file1. {}", e);
            return ExitCode::FAILURE;
        }
    };
    let mut reader2 = match build_reader(file2, args.file2_format.into()) {
        Ok(r) => r,
        Err(e) => {
            error!("failed to create reader from file2. {}", e);
            return ExitCode::FAILURE;
        }
    };

    let files_are_identical = loop {
        let record_result1 = reader1.produce_record();
        let record_result2 = reader2.produce_record();
        match (record_result1, record_result2) {
            (Some(result1), Some(result2)) => {
                let record1 = match result1 {
                    Ok(r1) => r1,
                    Err(e) => {
                        info!("Failed to get record from file1. {}", e);
                        break false;
                    }
                };
                let record2 = match result2 {
                    Ok(r2) => r2,
                    Err(e) => {
                        info!("Failed to get record from file2. {}", e);
                        break false;
                    }
                };
                if record1 != record2 {
                    info!(
                        "record from file1 {} not equal to record {} from file2",
                        record1, record2
                    );
                    break false;
                }
            }
            (Some(_), None) | (None, Some(_)) => break false,
            (_, _) => break true,
        }
    };
    if files_are_identical {
        info!("Data in files are identical");
    } else {
        warn!("Data in files are not identical");
    }
    ExitCode::SUCCESS
}
