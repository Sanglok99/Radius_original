// Not used

use std::time::{SystemTime, UNIX_EPOCH};

use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncEpoch {
    pub rollup_id: RollupId,
    pub epoch: u64,
}

impl RpcParameter<AppState> for SyncEpoch {
    type Response = ();

    fn method() -> &'static str {
        "sync_epoch"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        let rollup = Rollup::get(&self.rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        mut_cluster_metadata.epoch = Some(self.epoch);
        mut_cluster_metadata.update()?;

        tracing::info!(
            "Epoch synced - rollup_id: {:?}, epoch: {:?}",
            self.rollup_id,
            self.epoch
        );

        Ok(())
    }
}