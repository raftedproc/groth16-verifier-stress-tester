use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use abi::Abi;
use chrono::prelude::*;
use clap::Parser;
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::time::Instant;
use walkdir::WalkDir;

type Sample = (u64, u64);

#[allow(dead_code)]
#[derive(Deserialize)]
struct Proof {
    pi_a: [String; 3],
    pi_b: [[String; 2]; 3],
    pi_c: [String; 3],
    protocol: String,
    curve: String,
}

#[derive(Deserialize)]
struct Public {
    public: String,
}

#[derive(Parser, Debug, Clone)]
struct StressTestArgs {
    #[arg(short, long)]
    proof_dir_path: String,

    #[arg(short, long, default_value = "https://rpc.stage.fluence.dev/")]
    rpc_url: String,

    #[arg(short, long)]
    contract_address: String,

    // #[arg(short, long, default_value = "include_str!(\"verify_proof_abi.json\")")]
    // contract_abi_path: String,
    #[arg(short, long, default_value = "100")]
    number_of_runs_per_worker: usize,

    #[arg(short, long, default_value = "10")]
    number_of_workers: usize,

    #[arg(short, long, default_value = "false")]
    save_stats: bool,
}

async fn read_from_json_file<T: DeserializeOwned>(file_path: &str) -> serde_json::Result<T> {
    let mut file = File::open(file_path)
        .await
        .expect(format!("read_from_json_file(): File {} not found", file_path).as_str());
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .await
        .expect("There must be valid file contents");
    serde_json::from_slice(&contents)
}

async fn status_updater(args: StressTestArgs, req_counter: Arc<AtomicU64>) {
    loop {
        // WIP
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        let req_count = req_counter.load(std::sync::atomic::Ordering::SeqCst);
        let req_left = args.number_of_runs_per_worker * args.number_of_workers - req_count as usize;
        println!(
            "Количество обработанных запросов {}, осталось {}",
            req_count, req_left,
        );

        if req_left == 0 {
            break;
        }
    }
}

async fn async_task(
    task_id: usize,
    args: StressTestArgs,
    number_of_proofs: usize,
    samples: Arc<Mutex<Vec<Sample>>>,
    req_counter: Arc<AtomicU64>,
) {
    let provider = Provider::<Http>::try_from("https://rpc.stage.fluence.dev/")
        .expect("Failed to create provider");

    let contract_address: Address = args
        .contract_address
        .parse()
        .expect("There must be a valid contract address");

    let abi: Abi = serde_json::from_str(include_str!("verify_proof_abi.json"))
        .expect("There must be valid ABI for the contract");

    // let abi: Abi =
    //     serde_json::from_str(&args.contract_abi).expect("There must be valid ABI for the contract");
    let contract = Contract::new(contract_address, abi, Arc::new(provider.clone()));

    let mut proofs: Vec<Proof> = Vec::with_capacity(number_of_proofs);
    let mut publics: Vec<Public> = Vec::with_capacity(number_of_proofs);

    for id in 1..number_of_proofs {
        let proof_path_pref = format!("{}/pr{}", args.proof_dir_path, id.to_string());
        let proof_path = format!("{}/{}", proof_path_pref, "proof.json");
        let public = format!("{}/{}", proof_path_pref, "public.json");
        proofs.push(
            read_from_json_file(&proof_path)
                .await
                .expect("There must be valid proof"),
        );
        publics.push(
            read_from_json_file(&public)
                .await
                .expect("There must be valid public"),
        );
    }
    let mut rng = SmallRng::from_entropy();

    // let mut hist = Histogram::<u64>::new_with_bounds(25, 1000, 4).unwrap();
    let mut m = samples.lock().await;
    let samples = m.deref_mut();
    let start = Instant::now();

    for iter_id in 0..args.number_of_runs_per_worker {
        let random_value: usize = rng.gen_range(0..number_of_proofs - 1);
        let proof = &proofs[random_value];
        let public = &publics[random_value];

        let pi_a: [U256; 2] = [
            U256::from_str_radix(&proof.pi_a[0], 10).expect("Must be a valid U256"),
            U256::from_str_radix(&proof.pi_a[1], 10).expect("Must be a valid U256"),
        ];
        let pi_b: [[U256; 2]; 2] = [
            [
                U256::from_str_radix(&proof.pi_b[0][1], 10).expect("Must be a valid U256"),
                U256::from_str_radix(&proof.pi_b[0][0], 10).expect("Must be a valid U256"),
            ],
            [
                U256::from_str_radix(&proof.pi_b[1][1], 10).expect("Must be a valid U256"),
                U256::from_str_radix(&proof.pi_b[1][0], 10).expect("Must be a valid U256"),
            ],
        ];
        let pi_c: [U256; 2] = [
            U256::from_str_radix(&proof.pi_c[0], 10).expect("Must be a valid U256"),
            U256::from_str_radix(&proof.pi_c[1], 10).expect("Must be a valid U256"),
        ];
        let pub_signals: [U256; 1] =
            [U256::from_str_radix(&public.public, 10).expect("Must be a valid U256")];

        let method = contract
            .method::<_, bool>("verifyProof", (pi_a, pi_b, pi_c, pub_signals))
            .expect("There must be a valid method of the contract");

        let call_start_moment = start.elapsed().as_millis() as u64;

        let call_res = method.call().await;

        let duration_ms = start.elapsed().as_millis() as u64;
        samples.push((call_start_moment, duration_ms));

        req_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // hist.record(duration_ms)
        //     .expect("There must be a valid record");

        if call_res.is_err() {
            println!(
                "Ошибка при выполнении задачи {}. {}",
                task_id,
                call_res.err().unwrap().to_string()
            );
            continue;
        } else {
            let verify_res = call_res.unwrap();
            if !verify_res {
                println!("Задача {} {} вернула неверный результат.", task_id, iter_id);
            }
        }
    }

    // let p95 = hist.value_at_quantile(0.95);
    // println!(
    //     "Задача {} завершила выполнение 95 процентиль {}",
    //     task_id, p95
    // );
}

async fn proces_save_stats(
    start: &DateTime<Utc>,
    samples: &Vec<Arc<Mutex<Vec<Sample>>>>,
    save_stats: bool,
) {
    let mut hist = Histogram::<u64>::new_with_bounds(25, 10000, 4).unwrap();

    for worker_samples in samples {
        let m = worker_samples.lock().await;
        let worker_samples = m.deref();
        for (_, duration) in worker_samples {
            hist.record(*duration)
                .expect("There must be a valid record to process");
        }
    }

    let utc_timestamp = start.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let now: DateTime<Utc> = Utc::now();
    let current_utc_timestamp = now.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    println!("Запуск {} UTC", utc_timestamp);
    println!("Завершено {} UTC", current_utc_timestamp);

    println!("50 процентиль {}", hist.value_at_quantile(0.50));
    println!("75 процентиль {}", hist.value_at_quantile(0.75));
    println!("95 процентиль {}", hist.value_at_quantile(0.95));

    if save_stats {
        let utc_start_timestamp = start.format("%Y_%m_%d_%H_%M_%S").to_string();
        let file = File::create(format!("samples_{}.txt", utc_start_timestamp))
            .await
            .expect("There must be a valid file to save samples");
        let mut w = csv_async::AsyncWriter::from_writer(file);
        w.write_record(&["worker_id", "rust_instant_as_start_moment", "duration"])
            .await
            .expect("There must be a valid record to write");

        // Skip the fist element
        for (worker_id, worker_samples) in samples[1..].iter().enumerate() {
            let m = worker_samples.lock().await;
            let worker_samples = m.deref();
            for (instant, duration) in worker_samples {
                w.write_record(&[
                    worker_id.to_string(),
                    instant.to_string(),
                    duration.to_string(),
                ])
                .await
                .expect("There must be a valid record to write");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = StressTestArgs::parse();
    let save_stats = args.save_stats;
    let proof_dir_path = &args.proof_dir_path;
    let number_of_proofs = WalkDir::new(proof_dir_path)
        .max_depth(1)
        .into_iter()
        .count();
    println!("Количество пруфов {}", number_of_proofs);
    let start: DateTime<Utc> = Utc::now();

    let mut tasks = Vec::new();
    let samples = vec![
        Arc::new(Mutex::new(Vec::<Sample>::with_capacity(
            args.number_of_runs_per_worker
        )));
        args.number_of_workers + 1
    ];

    let req_counter = Arc::new(AtomicU64::new(0));

    for i in 1..=args.number_of_workers {
        let local_req_counter = req_counter.clone();
        let args = args.clone();
        let samples = samples[i].clone();
        let task = tokio::spawn(async move {
            async_task(i, args, number_of_proofs, samples, local_req_counter).await;
        });
        tasks.push(task);
    }

    let updater_req_counter = req_counter.clone();
    // Periodic status updater
    let task = tokio::spawn(async move {
        status_updater(args, updater_req_counter).await;
    });
    tasks.push(task);

    for task in tasks {
        task.await.unwrap();
    }
    println!("Завершение работы");
    proces_save_stats(&start, &samples, save_stats).await;

    Ok(())
}
