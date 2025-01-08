use anyhow::{Context, Result};
use log::{error, info};
use mongodb::options::IndexOptions;
use mongodb::IndexModel;
use mongodb::{bson::doc, options::ClientOptions, Client, Collection};
use serde::{Deserialize, Serialize};
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiConfirmedBlock,
};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};

// -----------------------
// Configuration
// -----------------------
const MONGODB_URI: &str = "mongodb://localhost:27017";
const DATABASE_NAME: &str = "solana_db";
const COLLECTION_NAME: &str = "SolanaFees";
const BASE_FEE: u64 = 5000;
const MAX_PARALLEL_REQUESTS: usize = 5;
const SOLANA_URL: &str = "https://api.mainnet-beta.solana.com";
const PROCESSED_SLOTS_FILE: &str = "processedSlots.json";

// -----------------------
// Data Structures
// -----------------------
#[derive(Debug, Serialize, Deserialize)]
struct SolanaFee {
    signature: String,
    slot: u64,
    timestamp: i64,
    fee: u64,
    compute_units_consumed: u64,
    base_fee: u64,
    priority_fee: u64,
    priority_fee_per_units_consumed: u64,
}

impl SolanaFee {
    fn new(
        signature: String,
        slot: u64,
        timestamp: i64,
        fee: u64,
        compute_units_consumed: u64,
        base_fee: u64,
        priority_fee: u64,
        priority_fee_per_units_consumed: u64,
    ) -> Self {
        SolanaFee {
            signature,
            slot,
            timestamp,
            fee,
            compute_units_consumed,
            base_fee,
            priority_fee,
            priority_fee_per_units_consumed,
        }
    }
}

// -----------------------
// Utility Functions
// -----------------------

/// Reads processed slots from the local JSON file.
/// Returns a HashSet of processed slot numbers.
fn load_processed_slots<P: AsRef<Path>>(path: P) -> Result<HashSet<u64>> {
    if path.as_ref().exists() {
        let data = fs::read_to_string(&path).with_context(|| "Reading processed slots file")?;
        let slots: HashSet<u64> =
            serde_json::from_str(&data).with_context(|| "Parsing processed slots JSON")?;
        Ok(slots)
    } else {
        Ok(HashSet::new())
    }
}

/// Saves processed slots to the local JSON file.
fn save_processed_slots<P: AsRef<Path>>(path: P, slots: &HashSet<u64>) -> Result<()> {
    let data = serde_json::to_string(&slots).with_context(|| "Serializing processed slots")?;
    fs::write(&path, data).with_context(|| "Writing processed slots to file")?;
    Ok(())
}

/// Initializes MongoDB and returns the collection handle.
async fn initialize_mongodb() -> Result<Collection<SolanaFee>> {
    let mut client_options = ClientOptions::parse(MONGODB_URI)
        .await
        .with_context(|| "Parsing MongoDB connection string")?;

    client_options.app_name = Some("SolanaFeeProcessor".to_string());

    let client = Client::with_options(client_options).with_context(|| "Creating MongoDB client")?;

    let db = client.database(DATABASE_NAME);

    let collection = db.collection::<SolanaFee>(COLLECTION_NAME);

    collection
        .create_index(
            IndexModel::builder()
                .keys(doc! { "slot": 1, "signature": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .with_context(|| "Creating index on MongoDB collection")?;

    info!("Connected to MongoDB and ensured indexes.");

    Ok(collection)
}

/// Builds a DynamoDB item from a transaction.
fn build_transaction_item(
    tx: &EncodedTransactionWithStatusMeta,
    block_height: u64,
    block_time: i64,
) -> Option<SolanaFee> {
    let fee = tx.meta.as_ref()?.fee;
    let compute_units_consumed = tx
        .meta
        .as_ref()?
        .compute_units_consumed
        .clone()
        .unwrap_or(0);

    let signatures = match &tx.transaction {
        EncodedTransaction::Json(tx_raw) => &tx_raw.signatures,
        EncodedTransaction::Accounts(tx_raw) => &tx_raw.signatures,
        _ => return None,
    };

    let base_fee = BASE_FEE * (signatures.len() as u64);
    let priority_fee = fee.saturating_sub(base_fee);
    let priority_fee_per_units_consumed = if compute_units_consumed > 0 {
        priority_fee / compute_units_consumed
    } else {
        0
    };

    let signature = signatures.get(0)?.to_string();

    Some(SolanaFee::new(
        signature,
        block_height,
        block_time,
        fee,
        compute_units_consumed,
        base_fee,
        priority_fee,
        priority_fee_per_units_consumed,
    ))
}

/// Fetches and processes a single slot's transactions.
async fn process_slot(
    slot: u64,
    connection: Arc<RpcClient>,
    collection: Arc<Collection<SolanaFee>>,
) -> Result<()> {
    let block = get_block(slot, connection).await?;

    let block_height = block.block_height.unwrap_or(0);
    let block_time = block.block_time.unwrap_or(0);
    let transactions = block.transactions.unwrap_or_default();

    let mut solana_fees = Vec::new();

    for tx in &transactions {
        if let Some(fee) = build_transaction_item(&tx, block_height, block_time) {
            solana_fees.push(fee);
        }
    }

    if !solana_fees.is_empty() {
        match collection.insert_many(solana_fees).await {
            Ok(insert_result) => {
                info!(
                    "Inserted {} transactions from slot {} into MongoDB.",
                    insert_result.inserted_ids.len(),
                    slot
                );
            }
            Err(e) => {
                error!("Failed to insert documents for slot {}: {}", slot, e);
            }
        }
    } else {
        info!("No transactions to insert for slot {}.", slot);
    }

    Ok(())
}

async fn get_block(slot: u64, connection: Arc<RpcClient>) -> Result<UiConfirmedBlock, ClientError> {
    let max_attempts = 5;
    let delay = Duration::from_secs(10);

    for attempt in 1..=max_attempts {
        match connection
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(block) => return Ok(block),
            Err(e) => {
                error!(
                    "Attempt {}/{} failed to fetch block {}: {}",
                    attempt, max_attempts, slot, e
                );

                if attempt < max_attempts {
                    sleep(delay).await;
                    continue;
                }

                return Err(e);
            }
        }
    }

    unreachable!()
}

// -----------------------
// Main Logic
// -----------------------

/// Main function that processes Solana blocks.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let collection = Arc::new(initialize_mongodb().await?);

    let connection = Arc::new(RpcClient::new_with_commitment(
        SOLANA_URL.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let processed_slots = Arc::new(Mutex::new(load_processed_slots(PROCESSED_SLOTS_FILE)?));

    let processed_slots_for_signal = Arc::clone(&processed_slots);
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for CTRL+C");
        info!("Signal received! Saving processed slots...");

        let processed_slots = processed_slots_for_signal.lock().unwrap();
        if let Err(e) = save_processed_slots(PROCESSED_SLOTS_FILE, &*processed_slots) {
            error!("Failed to save processed slots: {}", e);
        }

        std::process::exit(0);
    });

    loop {
        match connection.get_slot().await {
            Ok(latest_slot) => {
                info!("Latest slot from Solana: {}", latest_slot);

                let mut slot = latest_slot;

                let semaphore = std::sync::Arc::new(Semaphore::new(MAX_PARALLEL_REQUESTS));
                let total_blocks = Arc::new(Mutex::new(0u64));
                let start_time = Arc::new(Instant::now());

                while slot > 0 {
                    if processed_slots.lock().unwrap().contains(&slot) {
                        info!("Slot {} already processed. Skipping.", slot);
                        return Ok(());
                    }

                    let permit = semaphore.clone().acquire_owned().await;
                    let connection_clone = Arc::clone(&connection);
                    let collection_clone = Arc::clone(&collection);
                    let total_blocks = Arc::clone(&total_blocks);
                    let start_time = Arc::clone(&start_time);
                    let processed_slots = Arc::clone(&processed_slots);

                    tokio::spawn(async move {
                        let slot_start_time = Instant::now(); // Independent task start time

                        if let Err(e) = process_slot(slot, connection_clone, collection_clone).await
                        {
                            error!("Error processing slot {}: {}", slot, e);
                        }

                        let mut processed_slots_guard = processed_slots.lock().unwrap();
                        processed_slots_guard.insert(slot);

                        drop(permit);

                        {
                            let mut total_blocks_guard = total_blocks.lock().unwrap();
                            *total_blocks_guard += 1;
                        }

                        let elapsed_secs_slot = slot_start_time.elapsed().as_secs_f64();
                        let elapsed_secs = start_time.elapsed().as_secs_f64();
                        let average_bps = if elapsed_secs > 0.0 {
                            *total_blocks.lock().unwrap() as f64 / elapsed_secs
                        } else {
                            0.0
                        };

                        info!(
                            "Slot {} processed in {:?}. Total blocks: {}. Average BPS: {:.2}",
                            slot,
                            elapsed_secs_slot,
                            *total_blocks.lock().unwrap(),
                            average_bps
                        );
                    });

                    slot -= 1;

                    // Optional: Add a short delay to prevent tight looping
                    sleep(Duration::from_secs(1)).await;
                }
            }
            Err(e) => {
                error!("Error fetching latest slot: {}", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
