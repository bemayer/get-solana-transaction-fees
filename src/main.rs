use anyhow::{Context, Result};
use log::{error, info};
use mongodb::{
    bson::doc,
    options::{ClientOptions, IndexOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use solana_client::{
    client_error::ClientError, nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiConfirmedBlock,
};
use std::{sync::Arc, time::Instant};
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
};

// -----------------------
// Configuration Constants
// -----------------------
const MONGODB_URI: &str = "mongodb://localhost:27017";
const DATABASE_NAME: &str = "solana_db";
const COLLECTION_NAME: &str = "SolanaFees";
const BASE_FEE: u64 = 5000;
const REQUEST_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CONCURRENT_REQUESTS: usize = 4;
const SOLANA_URL: &str = "https://api.mainnet-beta.solana.com";

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
        Self {
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

/// Initializes MongoDB and ensures a composite index on `slot` and `signature`.
async fn initialize_mongodb() -> Result<Collection<SolanaFee>> {
    let mut client_options = ClientOptions::parse(MONGODB_URI)
        .await
        .with_context(|| "Parsing MongoDB connection string")?;

    client_options.app_name = Some("SolanaFeeProcessor".to_string());

    let client = Client::with_options(client_options).with_context(|| "Creating MongoDB client")?;

    let collection = client.database(DATABASE_NAME).collection::<SolanaFee>(COLLECTION_NAME);

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

/// Converts a transaction into a `SolanaFee` item.
fn build_transaction_item(
    tx: &EncodedTransactionWithStatusMeta,
    block_height: u64,
    block_time: i64,
) -> Option<SolanaFee> {
    let meta = tx.meta.as_ref()?;

    let fee = meta.fee;
    let compute_units_consumed = meta.compute_units_consumed.clone().unwrap_or(0);

    let signatures = match &tx.transaction {
        EncodedTransaction::Json(tx_raw) => &tx_raw.signatures,
        EncodedTransaction::Accounts(tx_raw) => &tx_raw.signatures,
        _ => return None,
    };

    let base_fee = BASE_FEE * signatures.len() as u64;
    let priority_fee = fee.saturating_sub(base_fee);
    let priority_fee_per_units_consumed = if compute_units_consumed > 0 {
        priority_fee / compute_units_consumed
    } else {
        0
    };

    let signature = signatures.get(0)?.clone();

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

/// Checks if a slot and signature combination is already processed.
async fn is_slot_processed(
    slot: u64,
    signature: &str,
    collection: Arc<Collection<SolanaFee>>,
) -> Result<bool> {
    let filter = doc! { "slot": slot as i64 };
    collection
        .find_one(filter)
        .await
        .map(|result| result.is_some())
        .with_context(|| {
            format!(
                "Checking if slot {} and signature {} are processed",
                slot, signature
            )
        })
}

/// Processes a single slot and stores its transactions in MongoDB.
async fn process_slot(
    slot: u64,
    connection: Arc<RpcClient>,
    collection: Arc<Collection<SolanaFee>>,
) -> Result<()> {
    let block = get_block(slot, connection).await?;

    let block_height = block.block_height.unwrap_or(0);
    let block_time = block.block_time.unwrap_or(0);
    let transactions = block.transactions.unwrap_or_default();

    let solana_fees: Vec<SolanaFee> = transactions
        .iter()
        .filter_map(|tx| build_transaction_item(tx, block_height, block_time))
        .collect();

    if solana_fees.is_empty() {
        info!("No transactions to insert for slot {}.", slot);
        return Ok(());
    }

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

    Ok(())
}

/// Fetches a block with retries.
async fn get_block(slot: u64, connection: Arc<RpcClient>) -> Result<UiConfirmedBlock, ClientError> {
    let mut attempts = 0;

    loop {
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
                    "Failed to fetch block {} (attempt {}): {}",
                    slot, attempts, e
                );
                if attempts < 5 {
                    sleep(Duration::from_secs(10)).await;
                    attempts += 1;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

// -----------------------
// Main Logic
// -----------------------

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let collection = Arc::new(initialize_mongodb().await?);
    let connection = Arc::new(RpcClient::new_with_commitment(
        SOLANA_URL.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut last_request_time = Instant::now();


    loop {
        if let Ok(latest_slot) = connection.get_slot().await {
            info!("Latest slot: {}", latest_slot);

            let mut slot = latest_slot - 1;

            while slot > 0 {
                if is_slot_processed(slot, &slot.to_string(), Arc::clone(&collection)).await? {
                    info!("Slot {} already processed. Skipping...", slot);
                    slot -= 1;
                    continue;
                }

                let elapsed = last_request_time.elapsed();
                if elapsed < REQUEST_INTERVAL {
                    sleep(REQUEST_INTERVAL - elapsed).await;
                }
                last_request_time = Instant::now();

                let connection_clone = Arc::clone(&connection);
                let collection_clone = Arc::clone(&collection);
                let permit = semaphore.clone().acquire_owned().await;

                tokio::spawn(async move {
                    if let Err(e) = process_slot(slot, connection_clone, collection_clone).await {
                        error!("Failed to process slot {}: {}", slot, e);
                    } else {
                        info!("Processed slot {}", slot);
                    }
                    drop(permit);
                });

                slot -= 1;
            }
        } else {
            error!("Failed to fetch the latest slot. Retrying...");
            sleep(Duration::from_secs(5)).await;
        }
    }
}
