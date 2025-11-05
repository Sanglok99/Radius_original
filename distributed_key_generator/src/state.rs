use std::sync::Arc;

use crate::types::Config;

pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: Config,
    skde_params: skde::delay_encryption::SkdeParams,
}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl AppState {
    pub fn new(config: Config, skde_params: skde::delay_encryption::SkdeParams) -> Self {
        let inner = AppStateInner {
            config,

            skde_params,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn skde_params(&self) -> &skde::delay_encryption::SkdeParams {
        &self.inner.skde_params
    }
}
