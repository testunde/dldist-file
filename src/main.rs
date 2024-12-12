use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufRead};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

use clap::Parser;
use num_traits::PrimInt;
use threadpool::ThreadPool;

struct DistanceResult {
    line_a: u32,
    line_b: u32,
    _mean_line_len: f32,
    dldist: u32,
    normalized_dldist: f32,
}

const NUM_PRINT_ALL: u16 = 0;
const NUM_ALL_THREADS_AVAILBLE: usize = 0;

static VERBOSE: Mutex<bool> = Mutex::new(false);
static THREAD_NUM: Mutex<usize> = Mutex::new(1);

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

    if *VERBOSE.lock().unwrap() {
        // print beautified 2D-matrix
        println!("{}", format!("{:?}", dist).replace("], [", "],\n["));
    }

    return dist[str_a.len()][str_b.len()];
}

use std::sync::mpsc::channel;
fn calculate_osa_distances(lines: &Vec<String>) -> Vec<DistanceResult> {
    let lines_cnt = lines.len();

    let pool = ThreadPool::new(*THREAD_NUM.lock().unwrap());

    let rx = {
        // required so that tx on main thread is dropped and rx.iter() does not block
        let (tx, rx) = channel::<DistanceResult>();

        // notifying variables to wake up main thread
        let pair = Arc::new((Mutex::new(()), Condvar::new()));
        let (lock, cvar) = &*pair;

        for la in 0..lines_cnt {
            for lb in la..lines_cnt {
                if la == lb {
                    // ignore self-comparison
                    continue;
                }
                let line_a = lines[la].clone();
                let line_b = lines[lb].clone();
                let tx_child = tx.clone();
                let pair_child = Arc::clone(&pair);
                pool.execute(move || {
                    let distance = calculate_osa_distance_between_two_strings(&line_a, &line_b);
                    let mean_line_length = ((line_a.len() as f32) + (line_b.len() as f32)) * 0.5f32;
                    tx_child
                        .send(DistanceResult {
                            line_a: la as u32,
                            line_b: lb as u32,
                            _mean_line_len: mean_line_length,
                            dldist: distance,
                            normalized_dldist: (distance as f32) / mean_line_length,
                        })
                        .unwrap();

                    // We notify the condvar that we are done with calculating.
                    let (lock_child, cvar_child) = &*pair_child;
                    let _guard = lock_child.lock().unwrap();
                    cvar_child.notify_one();
                });

                {
                    // This prevents from spamming the queue and thus the memory.
                    // That way it makes sure the current+queued jobs are twice the set thread count.
                    let mut _guard = lock.lock().unwrap();
                    while pool.queued_count() >= pool.max_count() {
                        _guard = cvar.wait(_guard).unwrap();
                    }
                }
            }
        }
        rx
    };
    pool.join();

    rx.iter().collect()
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

    /// Normalizes the resulting distance by the mean lengths of the lines in the pair. This value is used for sorted output instead.
    #[arg(long)]
    normalize: bool,

    /// Optionally parallelize the calculations with multiple threads. N=1 means single-threaded.
    /// Set to N=0 to utilize all-but-one available cores of the running system.
    #[arg(short = 'j', long, default_value_t = 1usize)]
    thread_num: usize,

    /// Also print the two lines between which the distance has been calculated as shown in the end result list.
    #[arg(short = 'p', long)]
    print_lines: bool,

    /// Print additional info
    #[arg(short = 'v', long)]
    verbose: bool,
}

fn main() {
    // argument parsing & handling
    let args = Arguments::parse();
    *VERBOSE.lock().unwrap() = args.verbose;

    if args.thread_num == NUM_ALL_THREADS_AVAILBLE {
        let res = thread::available_parallelism();
        if res.is_err() {
            println!(
                "WARN: Could not determine thread count from running system. Setting thread_num=1."
            )
        }

        // 2!=0 thus unwrap would not panic
        *THREAD_NUM.lock().unwrap() =
            res.unwrap_or(NonZero::<usize>::new(1 + 1).unwrap()).get() - 1;
    } else {
        *THREAD_NUM.lock().unwrap() = args.thread_num;
    }
    println!("Running with {} threads.", *THREAD_NUM.lock().unwrap());

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
    let start_time = Instant::now();
    let mut distance_results = calculate_osa_distances(&lines);
    if distance_results.len() as u32 != combinations_cnt {
        panic!("Somehow the size of the result combinations list ({}) does not equal the theoretical count ({})!?",
            distance_results.len(),
            combinations_cnt);
    }
    println!(
        "Calculations done within {:.4}s (without sorting).",
        start_time.elapsed().as_secs_f32()
    );
    // sort depending on user settings
    if args.normalize {
        if args.descending {
            distance_results.sort_by(|a, b| {
                b.normalized_dldist
                    .partial_cmp(&a.normalized_dldist)
                    .unwrap_or(Ordering::Equal)
            });
        } else {
            distance_results.sort_by(|a, b| {
                a.normalized_dldist
                    .partial_cmp(&b.normalized_dldist)
                    .unwrap_or(Ordering::Equal)
            });
        }
    } else {
        if args.descending {
            distance_results.sort_by(|a, b| b.dldist.cmp(&a.dldist));
        } else {
            distance_results.sort_by(|a, b| a.dldist.cmp(&b.dldist));
        }
    }

    let print_cnt_limit = combinations_cnt.min(args.n_pairs as u32);
    println!(
        "==> Printing{} {} results in {} order:",
        if args.normalize { " normalized" } else { "" },
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
            "Line {: >4} vs. {: >4}: {}",
            dr.line_a + 1,
            dr.line_b + 1,
            if args.normalize {
                format!(
                    "norm. {:2.4} (dist. {: >3})",
                    dr.normalized_dldist, dr.dldist
                )
            } else {
                format!("{: >3}", dr.dldist)
            }
        );

        if args.print_lines {
            println!("{: >4}> {}", dr.line_a + 1, lines[dr.line_a as usize]);
            println!("{: >4}> {}", dr.line_b + 1, lines[dr.line_b as usize]);
            println!();
        }
    }
}
