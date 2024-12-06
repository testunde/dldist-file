use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use clap::Parser;

/// Returns an Iterator to the Reader of the lines of the file.
/// Preserves order and count of the raw file lines.
fn read_lines<P>(filename: P) -> io::Result<Vec<String>>
// fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let lines = io::BufReader::new(file).lines();
    let lines_filtered: Vec<_> = lines
        .map(|i| i.expect(""))
        // .filter(|x| !x.trim().is_empty()) // -> do not filter for emtpy lines here as otherwise the line numbers would not match those of the raw input file!
        .collect();
    Ok(lines_filtered)
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Input raw text file to analyse.
    #[clap(required = true)]
    input_file: PathBuf,

    /// List the results in descending order (default is ascending for viewing equal-like lines first)
    #[arg(short = 'd', long)]
    descending: bool,

    /// List only the top N pairs of lines depending on order direction. Set to 0 to list all pairs.
    #[arg(short = 'n', long, default_value_t = 10)]
    n_pairs: u16,

    /// Print additional info
    #[arg(short = 'v', long)]
    verbose: bool,
}

fn main() {
    // argument parsing & handling
    let args = Arguments::parse();

    println!(
        "==> Reading in '{}'...",
        match args.input_file.to_str() {
            Some(s) => s,
            None => panic!("Failed to build string from PathBuf (input file)!"),
        }
    );
    let _lines = match read_lines(args.input_file) {
        Ok(lns) => lns,
        Err(error) => panic!("Failed to read in lines from file: {error:?}"),
    };

    println!("==> Calculating Damerau-Levenshtein distances...");
    // TODO: also print combinatorial amount of distances to calculate
    // TODO: calculate

    println!(
        "==> Printing {} results:",
        if args.n_pairs == 0 {
            String::from("all")
        } else {
            format!("top {}", args.n_pairs)
        }
    );
    // TODO: sort
    // TODO: print
}
