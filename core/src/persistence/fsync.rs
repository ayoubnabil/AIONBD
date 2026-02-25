use std::fs::{self, File, OpenOptions};
use std::path::Path;

use super::PersistenceError;

pub(super) fn ensure_parent_dir(path: &Path) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

pub(super) fn truncate_file_fully_synced(path: &Path) -> Result<(), PersistenceError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn sync_parent_dir(path: &Path) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
