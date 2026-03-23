use crate::types::RawEvent;
use async_trait::async_trait;
use futures_util::StreamExt;
use prost_types::FieldMask;
use sui_rpc::Client;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsRequest;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;
use tonic::Streaming;

#[async_trait]
pub trait EventSource: Send {
    async fn next_events(&mut self) -> Vec<RawEvent>;
}

pub struct CheckpointEventSource {
    client: Client,
    stream: Option<Streaming<SubscribeCheckpointsResponse>>,
}

impl CheckpointEventSource {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            stream: None,
        }
    }
}

#[async_trait]
impl EventSource for CheckpointEventSource {
    async fn next_events(&mut self) -> Vec<RawEvent> {

        if self.stream.is_none() {

            let stream_subscription_read_mask: FieldMask = FieldMask {
                paths: vec![
                    "sequence_number".to_string(),
                    "transactions.digest".to_string(),
                    "transactions.events".to_string(),
                    "transactions.timestamp".to_string(),
                ],
            };

            let subscribe_checkpoints_request = SubscribeCheckpointsRequest::default()
                .with_read_mask(stream_subscription_read_mask);

            let mut subscription = self.client.subscription_client();
            match subscription
                .subscribe_checkpoints(subscribe_checkpoints_request)
                .await
            {
                Ok(r) => self.stream = Some(r.into_inner()),
                Err(e) => {
                    println!("Subscription error: {}", e);
                    return Vec::new();
                }
            }
        }
        
        let stream = if let Some(stream) = self.stream.as_mut() {
            stream
        } else {
            return Vec::new();
        };

        let (sequence_number, transactions) = match stream.next().await {
            Some(Ok(mut response)) => {
                let checkpoint = response.checkpoint_mut();
                (
                    checkpoint.sequence_number(),
                    std::mem::take(checkpoint.transactions_mut()),
                )
            }
            _ => {
                self.stream = None;
                return Vec::new();
            }
        };

        let mut events = Vec::new();

        for tx in transactions.into_iter() {
            if let Some(event_list) = tx.events {
                for event in event_list.events.into_iter() {
                    events.push(RawEvent {
                        checkpoint_sequence_number: sequence_number,
                        package_id: event.package_id.unwrap_or_default(),
                        module: event
                            .event_type
                            .as_ref()
                            .and_then(|t| t.split("::").nth(1))
                            .unwrap_or_default()
                            .to_string(),
                        event_function: event
                            .event_type
                            .as_ref()
                            .and_then(|t| t.split("::").nth(2))
                            .unwrap_or_default()
                            .to_string(),
                        event_type: event.event_type.unwrap_or_default(),
                        contents: event
                            .contents
                            .and_then(|c| c.value)
                            .map(|b| b.to_vec())
                            .unwrap_or_default(),
                    });
                }
            }
        }

        events
    }
}
