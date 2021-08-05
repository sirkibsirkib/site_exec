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

use core::hash::Hash;
use crossbeam_channel::{Receiver, Sender};
use ed25519_dalek::{ed25519, Keypair, PublicKey, Signature, Signer, Verifier};

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
struct SiteId(PublicKey);

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct AssetId(u32);

/// Message structure communicated between sites (over channels)
#[derive(Debug)]
enum Msg {
    AssetDataRequest { asset_id: AssetId }, // requester is implicit because messages are signed
    AssetData { asset_id: AssetId, asset_data: AssetData },
}
#[derive(Debug)]
struct SignedMsg {
    sender_public_key: PublicKey,
    signature: Signature,
    msg: Msg,
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
    keypair: Keypair,
    outboxes: Arc<HashMap<SiteId, Sender<SignedMsg>>>,
    asset_store: HashMap<AssetId, AssetData>,
    inbox: Receiver<SignedMsg>,
    last_requested_at: HashMap<AssetId, Instant>, // alternative: Sorted vector of (Instant, AssetId).
    logger: Box<dyn Logger>,
}

#[derive(Debug)]
struct Site {
    inner: SiteInner,
    todo_instructions: Vec<Instruction>, // Order is irrelevant. Using a vector because its easily iterable.
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

trait Logger: std::fmt::Debug + Send {
    fn line_writer(&mut self) -> Option<&mut dyn Write>;
}

#[derive(Debug)]
struct FileLogger {
    file: std::fs::File,
}
////////////////////////////////////////////////
fn any_as_u8_slice<T: Sized>(thing: &T) -> &[u8] {
    // source: https://stackoverflow.com/questions/28127165/how-to-convert-struct-to-u8
    unsafe {
        // safe! will certainly only read initialized memory
        std::slice::from_raw_parts(thing as *const T as *const u8, std::mem::size_of::<T>())
    }
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
