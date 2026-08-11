use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

#[derive(Default)]
pub(crate) struct CallIdMapper {
    active: Mutex<HashMap<String, String>>,
}

impl CallIdMapper {
    pub(crate) fn start_approval(&self, provider_call_id: &str) -> String {
        let handle = new_handle();
        lock_active(&self.active).insert(provider_call_id.to_owned(), handle.clone());
        handle
    }

    pub(crate) fn start_event(&self, provider_call_id: &str) -> String {
        let mut active = lock_active(&self.active);
        active
            .entry(provider_call_id.to_owned())
            .or_insert_with(new_handle)
            .clone()
    }

    pub(crate) fn finish(&self, provider_call_id: &str) -> String {
        lock_active(&self.active)
            .remove(provider_call_id)
            .unwrap_or_else(new_handle)
    }

    pub(crate) fn clear(&self) {
        lock_active(&self.active).clear();
    }
}

fn new_handle() -> String {
    format!("call-{}", Uuid::new_v4())
}

fn lock_active(active: &Mutex<HashMap<String, String>>) -> MutexGuard<'_, HashMap<String, String>> {
    active
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::CallIdMapper;

    #[test]
    fn provider_literals_are_never_used_as_outward_handles() {
        let mapper = CallIdMapper::default();

        let handle = mapper.start_event("redacted-call-0");

        assert_ne!(handle, "redacted-call-0");
        assert_eq!(mapper.finish("redacted-call-0"), handle);
    }
}
