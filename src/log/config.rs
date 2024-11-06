
pub(crate) struct Config {
    pub(crate) segment_config: SegmentConfig
}

pub(crate) struct SegmentConfig {
    pub(crate) max_index_bytes: usize,
    pub(crate) max_store_bytes: usize,
    pub(crate) initial_offset: usize,
}