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
    AssetDataRequest { asset_id: AssetId }, // requester is implicit now that messages are signed
    AssetData { asset_id: AssetId, asset_data: AssetData },
}
#[derive(Debug)]
struct SignedMsg {
    header: SignedMsgHeader,
    msg: Msg,
}
#[derive(Debug)]
struct SignedMsgHeader {
    sender_public_key: PublicKey,
    signature: Signature,
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

////////////////////////////////////////////////
impl SiteId {
    fn from_public_key_ref(public_key: &PublicKey) -> &Self {
        unsafe {
            //safe!
            core::mem::transmute(public_key)
        }
    }
    fn to_public_key_ref(&self) -> &PublicKey {
        unsafe {
            //safe!
            core::mem::transmute(&self)
        }
    }
}
impl Hash for SiteId {
    fn hash<H: core::hash::Hasher>(&self, h: &mut H) {
        self.0.as_bytes().hash(h)
    }
}
impl Msg {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            // safe!
            std::slice::from_raw_parts(
                self as *const Msg as *const u8,
                core::mem::size_of::<Self>(),
            )
        }
    }
    fn sign(self, keypair: &Keypair) -> SignedMsg {
        let signature = keypair.sign(self.as_slice());
        SignedMsg {
            header: SignedMsgHeader { sender_public_key: keypair.public, signature },
            msg: self,
        }
    }
}
impl SignedMsg {
    fn verify(&self) -> Result<(), ed25519::Error> {
        self.header.sender_public_key.verify(self.msg.as_slice(), &self.header.signature)
    }
    fn sender(&self) -> &SiteId {
        SiteId::from_public_key_ref(&self.header.sender_public_key)
    }
}
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
impl std::fmt::Debug for SiteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.as_bytes() {
            write!(f, "{:X}", byte)?;
        }
        Ok(())
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
