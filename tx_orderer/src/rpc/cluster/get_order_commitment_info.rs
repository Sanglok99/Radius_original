use radius_sdk::json_rpc::server::ProcessPriority;

use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetOrderCommitmentInfo {
    pub rollup_id: RollupId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetOrderCommitmentInfoResponse {
    pub batch_number: u64,
    pub transaction_order: u64,
}

impl RpcParameter<AppState> for GetOrderCommitmentInfo {
    type Response = GetOrderCommitmentInfoResponse;

    fn method() -> &'static str {
        "get_order_commitment_info"
    }

    fn priority(&self) -> ProcessPriority {
        ProcessPriority::High
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        println!("=== GetOrderCommitmentInfo 시작 ==="); // test code

        let rollup_metadata = RollupMetadata::get(&self.rollup_id)?;

        println!("rollup_metadata: {:?}", rollup_metadata); // test code
        println!("rollup_metadata.batch_number: {:?}", rollup_metadata.batch_number); // test code
        println!("rollup_metadata.transaction_order: {:?}", rollup_metadata.transaction_order); // test code

        tracing::info!(
            "Get order commitment info: rollup_id = {}, batch_number = {}, transaction_order = {}",
            self.rollup_id,
            rollup_metadata.batch_number,
            rollup_metadata.transaction_order
        );

        println!("=== GetOrderCommitmentInfo 종료 ==="); // test code

        Ok(GetOrderCommitmentInfoResponse {
            batch_number: rollup_metadata.batch_number,
            transaction_order: rollup_metadata.transaction_order,
        })
    }
}
