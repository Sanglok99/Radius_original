use crate::rpc::eth::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EthEstimateGas(Value);

impl RpcParameter<AppState> for EthEstimateGas {
    type Response = Value;

    fn method() -> &'static str {
        "eth_estimateGas"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        super::forward(Self::method(), self, context).await
    }
}
