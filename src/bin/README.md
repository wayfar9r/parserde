Comparer bin

A simple program that compares records from two files in different format

usage example:

RUST_LOG=info cargo run --bin comparer -- --file1 filepath --file1-format csv --file2 filepath --file2-format bin

Converter bin

A simple program that converts records from one format to another

usage example:

cargo run --bin converter -- --input filepath --input-format csv --output-format bin > output.bin
