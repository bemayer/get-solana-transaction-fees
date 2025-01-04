use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest, WriteRequest,
};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use futures::future::join_all;
use log::{error, info, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedTransaction, EncodedTransactionWithStatusMeta};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::{sleep, Instant};

// -----------------------
// Configuration
// -----------------------
const TABLE_NAME: &str = "SolanaFees";
const BASE_FEE: u64 = 5000;
const BATCH_SIZE: usize = 25;
const SOLANA_URL: &str = "https://api.mainnet-beta.solana.com";
const PROCESSED_SLOTS_FILE: &str = "../processedSlots.json";

// -----------------------
// Data Structures
// -----------------------

#[derive(Debug)]
struct DynamoDbItem {
    signature: AttributeValue,
    slot: AttributeValue,
    timestamp: AttributeValue,
    fee: AttributeValue,
    compute_units_consumed: AttributeValue,
    base_fee: AttributeValue,
    priority_fee: AttributeValue,
    priority_fee_per_units_consumed: AttributeValue,
}

impl DynamoDbItem {
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
        DynamoDbItem {
            signature: AttributeValue::S(signature),
            slot: AttributeValue::N(slot.to_string()),
            timestamp: AttributeValue::N(timestamp.to_string()),
            fee: AttributeValue::N(fee.to_string()),
            compute_units_consumed: AttributeValue::N(compute_units_consumed.to_string()),
            base_fee: AttributeValue::N(base_fee.to_string()),
            priority_fee: AttributeValue::N(priority_fee.to_string()),
            priority_fee_per_units_consumed: AttributeValue::N(
                priority_fee_per_units_consumed.to_string(),
            ),
        }
    }

    fn to_put_request(&self) -> Result<WriteRequest> {
        let mut item = std::collections::HashMap::new();
        item.insert("Signature".to_string(), self.signature.clone());
        item.insert("Slot".to_string(), self.slot.clone());
        item.insert("Timestamp".to_string(), self.timestamp.clone());
        item.insert("Fee".to_string(), self.fee.clone());
        item.insert(
            "ComputeUnitsConsumed".to_string(),
            self.compute_units_consumed.clone(),
        );
        item.insert("BaseFee".to_string(), self.base_fee.clone());
        item.insert("PriorityFee".to_string(), self.priority_fee.clone());
        item.insert(
            "priorityFeePerUnitsConsumed".to_string(),
            self.priority_fee_per_units_consumed.clone(),
        );

        let put_request = PutRequest::builder()
            .set_item(Some(item))
            .build()
            .map_err(|e| anyhow!("Error building PutRequest: {}", e))?;

        Ok(WriteRequest::builder().put_request(put_request).build())
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

/// Ensures the DynamoDB table exists. Creates it if it does not.
async fn ensure_table_exists(dynamo_client: &DynamoDbClient) -> Result<()> {
    match dynamo_client
        .describe_table()
        .table_name(TABLE_NAME)
        .send()
        .await
    {
        Ok(_) => {
            info!("Table \"{}\" already exists.", TABLE_NAME);
        }
        Err(e) => match e.as_service_error() {
            Some(e) if e.is_resource_not_found_exception() => {
                create_table(dynamo_client).await?;
            }
            _ => {
                return Err(anyhow!("Error describing table: {}", e));
            }
        },
    }
    Ok(())
}

/// Creates the DynamoDB table.
async fn create_table(dynamo_client: &DynamoDbClient) -> Result<()> {
    let key_schema = KeySchemaElement::builder()
        .attribute_name("Signature")
        .key_type(KeyType::Hash)
        .build()?;

    dynamo_client
        .create_table()
        .table_name(TABLE_NAME)
        .key_schema(key_schema)
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .with_context(|| "Creating DynamoDB table")?;

    info!("Table \"{}\" is being created...", TABLE_NAME);

    // Wait until the table is active
    dynamo_client
        .wait_until_table_exists()
        .table_name(TABLE_NAME)
        .wait(Duration::from_secs(60))
        .await
        .with_context(|| "Waiting for table to become active")?;

    info!("Table \"{}\" is now active.", TABLE_NAME);
    Ok(())
}

/// Sends a batch of write requests to DynamoDB.
async fn send_batch(dynamo_client: &DynamoDbClient, write_requests: &[WriteRequest]) -> Result<()> {
    match dynamo_client
        .batch_write_item()
        .request_items(TABLE_NAME, write_requests.to_vec())
        .send()
        .await
    {
        Ok(result) => {
            if let Some(unprocessed_items) = result.unprocessed_items() {
                if let Some(unprocessed) = unprocessed_items.get(TABLE_NAME) {
                    if !unprocessed.is_empty() {
                        warn!("Some items were not processed. Consider implementing retry logic.");
                    }
                }
            }
        }
        Err(e) => {
            error!("Batch write error: {}", e);
        }
    }

    Ok(())
}

/// Builds a DynamoDB item from a transaction.
fn build_transaction_item(
    tx: &EncodedTransactionWithStatusMeta,
    block_height: u64,
    block_time: i64,
) -> Option<DynamoDbItem> {
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

    Some(DynamoDbItem::new(
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
    connection: &RpcClient,
    dynamo_client: &DynamoDbClient,
) -> Result<()> {
    let block = connection
        .get_block_with_config(
            slot,
            solana_client::rpc_config::RpcBlockConfig {
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("Could not fetch block {}", slot))
        .ok();

    if block.is_none() {
        info!("Block {} is null or not available.", slot);
        return Ok(());
    }

    let block_height = block.as_ref().map_or(0, |b| b.block_height.unwrap_or(0));
    let block_time = block.as_ref().map_or(0, |b| b.block_time.unwrap_or(0));
    let transactions = block.unwrap().transactions.unwrap_or_default();

    let mut write_requests = Vec::new();

    for tx in &transactions {
        if let Some(item) = build_transaction_item(&tx, block_height, block_time) {
            write_requests.push(item.to_put_request()?);
        }
    }

    // Chunk the write requests
    let batches: Vec<&[WriteRequest]> = write_requests.chunks(BATCH_SIZE).collect();

    // Send all batches concurrently
    let futures = batches.iter().map(|batch| {
        async move { send_batch(&dynamo_client, batch).await }
    });

    join_all(futures).await;

    Ok(())
}

// -----------------------
// Main Logic
// -----------------------

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Load AWS configuration
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo_client = DynamoDbClient::new(&aws_config);

    // Ensure DynamoDB table exists
    ensure_table_exists(&dynamo_client).await?;

    // Initialize Solana RPC client
    let connection =
        RpcClient::new_with_commitment(SOLANA_URL.to_string(), CommitmentConfig::confirmed());

    // Load processed slots
    let mut processed_slots = load_processed_slots(PROCESSED_SLOTS_FILE)?;

    // Initialize counters for timing
    let mut total_blocks: u64 = 0;
    let mut total_time = Duration::new(0, 0);

    loop {
        match connection.get_slot().await {
            Ok(latest_slot) => {
                info!("Latest slot from Solana: {}", latest_slot);

                // Start processing from the latest slot and go backwards
                let mut slot = latest_slot;

                while slot > 0 {
                    let start_time = Instant::now();

                    if processed_slots.contains(&slot) {
                        info!("Slot {} already processed. Skipping.", slot);
                        return Ok(());
                    }

                    if let Err(e) = process_slot(slot, &connection, &dynamo_client).await {
                        error!("Error processing slot {}: {}", slot, e);
                        // Depending on the error, decide to continue or break
                        slot -= 1;
                        continue;
                    }

                    processed_slots.insert(slot);
                    save_processed_slots(PROCESSED_SLOTS_FILE, &processed_slots)?;

                    let elapsed = start_time.elapsed();
                    total_time += elapsed;
                    total_blocks += 1;

                    // Calculate average blocks per second
                    let elapsed_secs = total_time.as_secs_f64();
                    let average_bps = if elapsed_secs > 0.0 {
                        total_blocks as f64 / elapsed_secs
                    } else {
                        0.0
                    };

                    info!(
                        "Finished processing slot {}. Time taken: {:.2?}. Average blocks/sec: {:.2}",
                        slot, elapsed, average_bps
                    );

                    slot -= 1;

                    // Optional: Add a short delay to prevent tight looping
                    // sleep(Duration::from_millis(100)).await;
                }
            }
            Err(e) => {
                error!("Error fetching latest slot: {}", e);
                // Wait before retrying
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
