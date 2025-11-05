use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetLatestKeyId {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetLatestKeyIdResponse {
    pub latest_key_id: KeyId,
}

impl RpcParameter<AppState> for GetLatestKeyId {
    type Response = GetLatestKeyIdResponse;

    fn method() -> &'static str {
        "get_latest_key_id"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        let key_id = KeyId::get()?;

        loop {
            return Ok(GetLatestKeyIdResponse {
                latest_key_id: key_id,
            });
        }
    }
}
