macro_rules! log {
    ($logger:expr, $($arg:tt)*) => {{
        if let Some(w) = $logger.line_writer() {
            let _ = writeln!(w, $($arg)*);
        }
    }};
}

mod planning;
mod scenario;
mod site;

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::Path,
};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct SiteId(u32);

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct AssetId(u32);

/// Message structure communicated between sites (over channels)
#[derive(Debug)]
enum Msg {
    AssetDataRequest { asset_id: AssetId, requester: SiteId },
    AssetData { asset_id: AssetId, asset_data: AssetData },
}

#[derive(Debug, Clone)]
struct AssetData {
    bits: u64,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ComputeArgs {
    inputs: Vec<AssetId>,
    outputs: Vec<AssetId>,
    compute_asset: AssetId,
}

#[derive(Debug, Clone)]
enum Instruction {
    SendAssetTo { asset_id: AssetId, site_id: SiteId },
    AcquireAssetFrom { asset_id: AssetId, site_id: SiteId },
    ComputeAssetData(ComputeArgs),
}

#[derive(Debug)]
struct SiteInner {
    // site_id_manager: SiteIdManager,
    site_id: SiteId,
    asset_store: HashMap<AssetId, AssetData>,
    inbox: mpsc::Receiver<Msg>,
    peer_outboxes: HashMap<SiteId, mpsc::Sender<Msg>>,
    last_requested_at: HashMap<AssetId, Instant>, // alternative: Sorted vector of (Instant, AssetId).
    logger: Box<dyn Logger>,
}

#[derive(Debug)]
struct Site {
    todo_instructions: Vec<Instruction>, // Order is irrelevant. Using a vector because its easily iterable.
    inner: SiteInner,
}

struct NetworkConfig {
    nodes: HashMap<SiteId, Box<dyn Logger>>,
    bidir_edges: Vec<[SiteId; 2]>,
}

#[derive(Debug)]
struct Problem {
    may_access: HashSet<(SiteId, AssetId)>,
    may_compute: HashSet<(SiteId, AssetId)>,
    site_has_asset: HashSet<(SiteId, AssetId)>,
    do_compute: Vec<ComputeArgs>, // outputs are implicit goals
}

#[derive(Debug)]
enum PlanError<'a> {
    CyclicCausality(&'a ComputeArgs),
    NoSiteForCompute(&'a ComputeArgs),
}
////////////////////////////////////////////////

impl ComputeArgs {
    fn needed_assets(&self) -> impl Iterator<Item = &AssetId> + '_ {
        self.inputs.iter().chain(Some(&self.compute_asset))
    }
}

impl std::fmt::Debug for AssetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AssetId").field(&self.0).finish()
    }
}

trait Logger: std::fmt::Debug + Send {
    fn line_writer(&mut self) -> Option<&mut dyn Write>;
}
#[derive(Debug)]
struct FileLogger {
    file: std::fs::File,
}
impl FileLogger {
    fn new(path: impl AsRef<Path>) -> Box<dyn Logger> {
        Box::new(Self { file: File::create(path).unwrap() }) as Box<dyn Logger>
    }
}
impl Logger for FileLogger {
    fn line_writer(&mut self) -> Option<&mut dyn Write> {
        write!(&mut self.file, ">> ").unwrap();
        Some(&mut self.file)
    }
}
////////////////////////////////////////////////

fn main() {
    scenario::scenario_amy_bob_cho()
}
