use std::path::Path;

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize)]
pub struct PersistedCache {
    pub entries: Vec<PersistedEntry>,
}

#[derive(Archive, Serialize, Deserialize, Clone)]
pub struct PersistedEntry {
    pub name: String,
    pub record_type: u16,
    pub response_bytes: Vec<u8>,
    pub cached_at_unix_secs: u64,
    pub ttl_secs: u64,
}

pub fn save_cache(entries: Vec<PersistedEntry>, path: &Path) -> std::io::Result<()> {
    let data = PersistedCache { entries };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&data)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    std::fs::write(path, bytes.as_slice())?;
    Ok(())
}

pub fn load_cache(path: &Path) -> std::io::Result<Vec<PersistedEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(path)?;
    let archived = rkyv::access::<ArchivedPersistedCache, rkyv::rancor::Error>(&bytes)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let mut result = Vec::new();
    for entry in archived.entries.iter() {
        result.push(PersistedEntry {
            name: entry.name.as_str().to_string(),
            record_type: entry.record_type.into(),
            response_bytes: entry.response_bytes.as_slice().to_vec(),
            cached_at_unix_secs: entry.cached_at_unix_secs.into(),
            ttl_secs: entry.ttl_secs.into(),
        });
    }
    Ok(result)
}
