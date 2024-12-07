use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use clap::Parser;
use num_traits::PrimInt;

struct DistanceResult {
    line_a: u32,
    line_b: u32,
    dldist: u32,
}

const NUM_PRINT_ALL: u16 = 0;

/// Returns an Iterator to the Reader of the lines of the file.
/// Preserves order and count of the raw file lines.
fn read_lines<P>(filename: P) -> io::Result<Vec<String>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let lines = io::BufReader::new(file).lines();
    let lines_filtered: Vec<_> = lines
        .map(|i| i.expect(""))
        // .filter(|x| !x.trim().is_empty()) // -> do not! filter for emtpy lines here as otherwise the line numbers would not match those of the raw input file!
        .collect();
    Ok(lines_filtered)
}

/// Returns the amount of pair-combinations
fn pair_combinations_count<T>(num: T) -> T
where
    T: PrimInt + std::convert::From<u32>,
{
    if num < 2u32.into() {
        0u32.into()
    } else {
        (num * (num - 1u32.into())) / 2u32.into()
    }
}

// implementation inspired from: https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance
fn calculate_osa_distance_between_two_strings(str_a: &str, str_b: &str) -> u32 {
    let mut dist = vec![vec![0u32; str_b.len() + 1]; str_a.len() + 1]; // making sure indexing is in correct order

    for i in 0..=str_a.len() {
        dist[i][0] = i as u32;
    }
    dist[0] = (0..=str_b.len() as u32).collect();

    // using bytes instead of chars since we can not be sure of only UTF-8 characters being included in the file
    let mut a_prior: u8 = 0x00; // actual initial value does not matter
    let mut b_prior: u8 = 0x00; // actual initial value does not matter
    for (i, a) in str_a.bytes().enumerate() {
        for (j, b) in str_b.bytes().enumerate() {
            let cost: u32 = if a == b { 0 } else { 1 };
            dist[i + 1][j + 1] = (dist[i][j + 1] + 1) // deletion
                .min(dist[i + 1][j] + 1) // insertion
                .min(dist[i][j] + cost); // substitution

            if i > 0 && j > 0 && a == b_prior && a_prior == b {
                // transposition
                dist[i + 1][j + 1] = dist[i + 1][j + 1].min(dist[i - 1][j - 1] + 1);
            }

            b_prior = b;
        }
        a_prior = a;
    }

    // println!("{}", format!("{:?}", dist).replace("], [", "],\n[")); // print beautified 2D-matrix

    return dist[str_a.len()][str_b.len()];
}

fn calculate_osa_distances(lines: &Vec<String>) -> Vec<DistanceResult> {
    let lines_cnt = lines.len();
    let combinations_cnt = pair_combinations_count(lines_cnt as u64);
    let mut results = Vec::with_capacity(combinations_cnt as usize);

    for la in 0..lines_cnt {
        let line_a = &lines[la];
        for lb in la..lines_cnt {
            if la == lb {
                // ignore self-comparison
                continue;
            }
            let line_b = &lines[lb];
            let distance = calculate_osa_distance_between_two_strings(line_a, line_b);
            results.push(DistanceResult {
                line_a: la as u32,
                line_b: lb as u32,
                dldist: distance,
            });
        }
    }

    results
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    // TODO: also accept conent from stdin ('-')
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
    // TODO: add flag for optional parallelization (and measure execution time)
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
    let lines = match read_lines(args.input_file) {
        Ok(lns) => lns,
        Err(error) => panic!("Failed to read in lines from file: {error:?}"),
    };
    let lines_cnt = lines.len();
    if lines_cnt < 2 {
        println!(
            "The file has to contain at least two lines! Counted {}.",
            lines_cnt
        );
        return;
    }

    let combinations_cnt = pair_combinations_count(lines_cnt as u32);
    println!(
        "==> Calculating {} Damerau-Levenshtein distances between {} lines...",
        combinations_cnt, lines_cnt
    );
    // calculate all distances
    let mut distance_results = calculate_osa_distances(&lines);
    if distance_results.len() as u32 != combinations_cnt {
        panic!("Somehow the size of the result combinations list ({}) does not equal the theoretical count ({})!?",
            distance_results.len(),
            combinations_cnt);
    }
    // sort depending on user settings
    if args.descending {
        distance_results.sort_by(|a, b| b.dldist.cmp(&a.dldist));
    } else {
        distance_results.sort_by(|a, b| a.dldist.cmp(&b.dldist));
    }

    let print_cnt_limit = combinations_cnt.min(args.n_pairs as u32);
    println!(
        "==> Printing {} results in {} order:",
        if args.n_pairs == NUM_PRINT_ALL {
            format!("all {}", combinations_cnt)
        } else {
            format!("top {}", print_cnt_limit)
        },
        if args.descending {
            "descending"
        } else {
            "ascending"
        }
    );
    let print_cnt = if args.n_pairs == NUM_PRINT_ALL {
        combinations_cnt
    } else {
        print_cnt_limit
    };
    for i in 0..print_cnt as usize {
        let dr = &distance_results[i];
        // print padded values
        println!(
            "Line {: >4} vs. {: >4}: {: >3}",
            dr.line_a + 1,
            dr.line_b + 1,
            dr.dldist
        );
    }
}
