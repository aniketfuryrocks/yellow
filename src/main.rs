use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use futures_util::StreamExt;

use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Semaphore,
};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    prelude::{
        subscribe_update::UpdateOneof, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdateBlockMeta,
        SubscribeUpdateSlot, SubscribeRequestFilterTransactions,
    },
    tonic::{codegen::http::Version, service::Interceptor},
};

const MAX_BLOCK_INDEXERS: usize = 10;
const GRPC_URL: &str = "http://127.0.0.0:10000";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

type Slot = u64;
type AnyhowJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;

/// Connect to yellow stone plugin using yellow stone gRpc Client
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = GeyserGrpcClient::connect(GRPC_URL, None::<&'static str>, None)?;

    let version = client.get_version().await?;
    println!("Version: {:?}", version);

    let (slot_tx, slot_rx) = mpsc::unbounded_channel(); // don't introduce back pressure
    let (block_tx, block_rx) = mpsc::unbounded_channel(); // don't introduce back pressure

    let slot_producer: AnyhowJoinHandle = tokio::spawn(subscribe(client, slot_tx, block_tx));
    let block_indexer: AnyhowJoinHandle = tokio::spawn(index_blocks(slot_rx));

    tokio::select! {
        _ = slot_producer => {
            bail!("Slot stream closed unexpectedly");
        }
        _ = block_indexer => {
            bail!("Block indexer closed unexpectedly");
        }
    }
}

pub async fn index_blocks(mut rx: UnboundedReceiver<Slot>) -> anyhow::Result<()> {
    let block_worker_semaphore = Arc::new(Semaphore::new(MAX_BLOCK_INDEXERS));

    while let Some(slot) = rx.recv().await {
        println!(
            "Block indexers running {:?}/MAX_BLOCK_INDEXERS",
            MAX_BLOCK_INDEXERS - block_worker_semaphore.available_permits()
        );

        let permit = block_worker_semaphore.clone().acquire_owned().await?;

        tokio::spawn(async move {
            index_block_for_slot(slot).await.unwrap();

            drop(permit);
        });
    }

    bail!("Slot stream closed unexpectedly");
}

pub async fn index_block_for_slot(slot: Slot) -> anyhow::Result<()> {
    println!("Processing slot: {:?}", slot);
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    Ok(())
}

pub async fn subscribe<F: Interceptor>(
    mut client: GeyserGrpcClient<F>,
    slot_tx: UnboundedSender<Slot>,
    block_tx: UnboundedSender<SubscribeUpdateBlockMeta>,
) -> anyhow::Result<()> {
    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocksMeta {
//            account_include: vec![SYSTEM_PROGRAM.to_string()],
        },
    );

    let mut blocks = HashMap::new();
    blocks.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: vec![
               SYSTEM_PROGRAM.to_string()
            ],
        },
    );

    let mut tx = HashMap::new(); 
    tx.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: Some(true),
            signature: None,
            account_exclude: None,
            account_required: todo!(),
            account_include: None,
        },
    );

    let mut stream = client
        .subscribe_once(
            slots,
            Default::default(),
            Default::default(),
            blocks,
            blocks_meta,
            None,
            Default::default(),
        )
        .await?;

    while let Some(message) = stream.next().await {
        let message = message?;

        let Some(update) = message.update_oneof else  {
            continue;
        };

        match update {
            UpdateOneof::Slot(SubscribeUpdateSlot { slot, .. }) => {
                slot_tx.send(slot)?;
            }
            UpdateOneof::BlockMeta(block_meta) => {
                block_tx.send(block_meta)?;
            }
            UpdateOneof::Block(block) => {
                println!("Block: {:?}", block);
            }
            UpdateOneof::Transaction(tx) => {
                println!("Transaction: {:?}", tx);
            }
            UpdateOneof::Ping(_) => {
                println!("Ping");
            }
            k => {
                bail!("Unexpected update: {k:?}");
            }
        };
    }

    bail!("Stream closed unexpectedly")
}
