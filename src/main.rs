use std::time::Duration;
use anyhow::{Context, Result};
use log::{error, info};
use mongodb::{
    bson::doc,
    options::{ClientOptions, IndexOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::EncodedTransactionWithStatusMeta;
use tokio::time::sleep;

// -----------------------
// Configuration Constants
// -----------------------
const MONGODB_URI: &str = "mongodb://localhost:27017";
const DATABASE_NAME: &str = "solana_db";
const COLLECTION_NAME: &str = "SolanaFees";
const BASE_FEE: i64 = 5000;
const SOLANA_URL: &str = "https://api.mainnet-beta.solana.com";
const REQUEST_INTERVAL_MS: u64 = 1500;

// -----------------------
// Data Structures
// -----------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct SlotData {
    pub slot: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_height: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TransactionFee>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionFee {
    pub signature: String,
    pub fee: i64,
    pub compute_units_consumed: i64,
    pub base_fee: i64,
    pub priority_fee: i64,
    pub priority_fee_per_units_consumed: i64,
}

// -----------------------
// Utility Functions
// -----------------------

/// Initializes MongoDB and ensures a unique index on the `slot` field.
async fn initialize_mongodb() -> Result<Collection<SlotData>> {
    let mut client_options = ClientOptions::parse(MONGODB_URI)
        .await
        .with_context(|| "Parsing MongoDB connection string")?;

    client_options.app_name = Some("SolanaFeeProcessor".to_string());

    let client = Client::with_options(client_options)
        .with_context(|| "Creating MongoDB client")?;

    let collection = client
        .database(DATABASE_NAME)
        .collection::<SlotData>(COLLECTION_NAME);

    collection
        .create_index(
            IndexModel::builder()
                .keys(doc! { "slot": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .with_context(|| "Creating unique index on 'slot' field")?;

    info!("Connected to MongoDB and ensured indexes.");
    Ok(collection)
}

/// Converts a Solana transaction into a `TransactionFee` item.
fn build_transaction_item(tx: &EncodedTransactionWithStatusMeta) -> Option<TransactionFee> {
    let meta = tx.meta.as_ref()?;
    let fee = meta.fee as i64;
    let compute_units_consumed = meta.clone().compute_units_consumed.unwrap_or(0) as i64;

    let signatures = match &tx.transaction {
        solana_transaction_status::EncodedTransaction::Json(tx_raw) => &tx_raw.signatures,
        solana_transaction_status::EncodedTransaction::Accounts(tx_raw) => &tx_raw.signatures,
        _ => return None,
    };

    let base_fee = BASE_FEE * signatures.len() as i64;
    let priority_fee = fee.saturating_sub(base_fee);
    let priority_fee_per_units_consumed = if compute_units_consumed > 0 {
        priority_fee / compute_units_consumed
    } else {
        0
    };

    Some(TransactionFee {
        signature: signatures.get(0)?.clone(),
        fee,
        compute_units_consumed,
        base_fee,
        priority_fee,
        priority_fee_per_units_consumed,
    })
}

/// Checks if a slot has been already processed.
async fn is_slot_processed(slot: i64, collection: &Collection<SlotData>) -> Result<bool> {
    let filter = doc! { "slot": slot };

    match collection.find_one(filter).await {
        Ok(result) => Ok(result.is_some()),
        Err(e) => {
            error!("Error checking slot {}: {:?}", slot, e);
            Err(e).with_context(|| format!("Checking if slot {} is processed", slot))
        }
    }
}

/// Builds a `SlotData` instance for a skipped slot.
fn build_skipped_slot_data(slot: i64, reason: &str) -> SlotData {
    SlotData {
        slot,
        block_height: None,
        timestamp: None,
        transactions: None,
        err: Some(reason.to_string()),
    }
}

/// Processes a single slot and stores its transactions in MongoDB.
async fn process_slot(
    slot: i64,
    connection: &RpcClient,
    collection: &Collection<SlotData>,
) -> Result<()> {
    match connection
        .get_block_with_config(
            slot as u64,
            RpcBlockConfig {
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
                ..Default::default()
            },
        )
        .await
    {
        Ok(block) => {
            let transactions: Vec<TransactionFee> = block
                .transactions
                .unwrap_or_default()
                .iter()
                .filter_map(|tx| build_transaction_item(tx))
                .collect();

            let slot_data = if transactions.is_empty() {
                build_skipped_slot_data(slot, "No transactions found.")
            } else {
                SlotData {
                    slot,
                    block_height: Some(block.block_height.unwrap_or(0) as i64),
                    timestamp: Some(block.block_time.unwrap_or(0)),
                    transactions: Some(transactions),
                    err: None,
                }
            };

            collection.insert_one(slot_data).await?;
        }
        Err(e) => {
            let skipped_data = build_skipped_slot_data(slot, &format!("Error: {}", e));
            collection.insert_one(skipped_data).await?;
        }
    }

    Ok(())
}

// -----------------------
// Main Logic
// -----------------------

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let collection = initialize_mongodb().await?;
    let connection = RpcClient::new_with_commitment(
        SOLANA_URL.to_string(),
        CommitmentConfig::confirmed(),
    );

    let mut slot = match std::env::args().nth(1) {
        Some(s) => s.parse::<i64>().context("Invalid slot number")?,
        None => connection.get_slot().await.context("Failed to fetch slot")? as i64 - 1,
    };

    loop {
        if is_slot_processed(slot, &collection).await? {
            slot -= 1;
            continue;
        }

        if let Err(e) = process_slot(slot, &connection, &collection).await {
            error!("Failed to process slot {}: {}", slot, e);
        } else {
            info!("Processed slot {}", slot);
        }

        sleep(Duration::from_millis(REQUEST_INTERVAL_MS)).await;
        slot -= 1;
    }
}
