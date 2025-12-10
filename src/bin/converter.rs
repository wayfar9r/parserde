use core::panic;
use std::{fs::File, io::stdout};

use clap::{Parser, ValueEnum};

use parserde::{build_reader, build_serializer, build_writer};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long)]
    input: String,
    #[arg(long)]
    input_format: InputFormat,
    #[arg(short, long)]
    output_format: OutputFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum InputFormat {
    Csv,
    Txt,
    Bin,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum OutputFormat {
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

impl From<OutputFormat> for &str {
    fn from(value: OutputFormat) -> Self {
        match value {
            OutputFormat::Csv => "csv",
            OutputFormat::Bin => "bin",
            OutputFormat::Txt => "txt",
        }
    }
}

fn main() {
    let args = Args::parse();
    let stdout = stdout();
    let file = match File::open(args.input) {
        Ok(f) => f,
        Err(e) => {
            panic!("failed to open an input file. {}", e);
        }
    };
    let mut reader = match build_reader(file, args.input_format.into()) {
        Ok(r) => r,
        Err(e) => {
            panic!("failed to create reader from input. {}", e)
        }
    };
    let serializer = match build_serializer(args.output_format.into()) {
        Ok(s) => s,
        Err(e) => {
            panic!("failed to create serializer. {}", e)
        }
    };
    let mut output_writer = match build_writer(stdout, args.output_format.into()) {
        Ok(w) => w,
        Err(e) => panic!("failed to create writer from stdout. {}", e),
    };

    if let Err(e) = output_writer.write_header() {
        panic!("failed to write header. {}", e);
    }

    while let Some(record_result) = reader.produce_record() {
        let record = match record_result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("an error occured while reading and parsing record. {}", e);
                continue;
            }
        };
        match serializer.serialize(&record) {
            Ok(result) => {
                if let Err(e) = output_writer.write(result) {
                    eprintln!("an error occured while writing data. {}", e);
                }
            }
            Err(e) => eprintln!("an error occured while serializing the record. {}", e),
        };
    }

    eprintln!("convert is successful");
}
