use core::time::Duration;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::mpsc;
use std::time::Instant;

macro_rules! log {
    ($logger:expr, $($arg:tt)*) => {{
        if let Some(w) = $logger.line_writer() {
            let _ = writeln!(w, $($arg)*);
        }
    }};
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct SiteId(u32);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct AssetIndex(u32);

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct AssetId {
    site_id: SiteId,
    asset_index: AssetIndex,
}

/// Message structure communicated between sites (over channels)
#[derive(Debug)]
enum Msg {
    AssetDataRequest { asset_id: AssetId, requester: SiteId },
    AssetData { asset_id: AssetId, asset_data: AssetData },
}

#[derive(Debug, Clone)]
struct AssetData;

#[derive(Debug)]
struct SiteIdManager {
    my_site_id: SiteId,
    asset_index_list: Vec<AssetIndex>,
    asset_index_seq_head: Option<AssetIndex>,
}

#[derive(Debug)]
enum Instruction {
    SendAssetTo { asset_id: AssetId, site_id: SiteId },
    AcquireAssetFrom { asset_id: AssetId, site_id: SiteId },
    ComputeAssetData { outputs: Vec<AssetId>, inputs: Vec<AssetId>, compute_asset: AssetId },
}

#[derive(Debug)]
struct SiteInner {
    site_id_manager: SiteIdManager,
    asset_store: HashMap<AssetId, AssetData>,
    inbox: mpsc::Receiver<Msg>,
    peer_outboxes: HashMap<SiteId, mpsc::Sender<Msg>>,
    last_requested_at: HashMap<AssetId, Instant>, // alternative: Sorted vector of (Instant, AssetId).
    logger: Box<dyn Logger>,
}

#[derive(Debug)]
struct Site {
    todo_instructions: Vec<Instruction>,
    inner: SiteInner,
}

struct NetworkConfig {
    nodes: HashMap<SiteId, Box<dyn Logger>>,
    bidir_edges: Vec<[SiteId; 2]>,
}

impl std::fmt::Debug for AssetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AssetId").field(&self.site_id.0).field(&self.asset_index.0).finish()
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
    fn new(path: impl AsRef<Path>) -> Self {
        Self { file: File::create(path).unwrap() }
    }
}
impl Logger for FileLogger {
    fn line_writer(&mut self) -> Option<&mut dyn Write> {
        write!(&mut self.file, ">> ").unwrap();
        Some(&mut self.file)
    }
}
////////////////////////////////////////////////

impl SiteIdManager {
    fn new(my_site_id: SiteId) -> Self {
        Self {
            my_site_id,
            asset_index_list: Default::default(),
            asset_index_seq_head: Some(AssetIndex(0)),
        }
    }
    fn alloc_asset_id(&mut self) -> Option<AssetId> {
        self.asset_index_list
            .pop()
            .or_else(|| {
                self.asset_index_seq_head.take().map(|AssetIndex(i)| {
                    if let Some(ip1) = i.checked_add(1) {
                        self.asset_index_seq_head = Some(AssetIndex(ip1));
                    }
                    AssetIndex(i)
                })
            })
            .map(|asset_index| AssetId { site_id: self.my_site_id, asset_index })
    }
    fn try_free_asset_id(&mut self, asset_id: AssetId) -> bool {
        if asset_id.site_id == self.my_site_id {
            false
        } else {
            self.asset_index_list.push(asset_id.asset_index);
            true
        }
    }
}

impl SiteInner {
    const REQUEST_PERIOD: Duration = Duration::from_millis(300);
    fn send_to(&mut self, dest_id: SiteId, msg: Msg) {
        log!(
            self.logger,
            "{:?} sending to {:?} msg {:?}",
            self.site_id_manager.my_site_id,
            dest_id,
            &msg
        );
        self.peer_outboxes.get(&dest_id).unwrap().send(msg).unwrap();
    }
    fn try_complete(&mut self, instruction: &mut Instruction) -> bool {
        match instruction {
            Instruction::AcquireAssetFrom { asset_id, site_id } => {
                if self.asset_store.contains_key(asset_id) {
                    return true;
                }
                let now = Instant::now();
                let recent_request = self
                    .last_requested_at
                    .get(asset_id)
                    .map(|&at| now - at < Self::REQUEST_PERIOD)
                    .unwrap_or(false);
                if !recent_request {
                    self.last_requested_at.insert(*asset_id, now);
                    let msg = Msg::AssetDataRequest {
                        asset_id: *asset_id,
                        requester: self.site_id_manager.my_site_id,
                    };
                    self.send_to(*site_id, msg);
                }
                false
            }
            Instruction::SendAssetTo { asset_id, site_id } => {
                if let Some(asset_data) = self.asset_store.get(&asset_id) {
                    let msg =
                        Msg::AssetData { asset_id: *asset_id, asset_data: asset_data.clone() };
                    self.send_to(*site_id, msg);
                    true
                } else {
                    false
                }
            }
            Instruction::ComputeAssetData { outputs, inputs, compute_asset } => {
                if inputs
                    .iter()
                    .copied()
                    .chain(Some(*compute_asset))
                    .all(|asset_id| self.asset_store.contains_key(&asset_id))
                {
                    log!(
                        self.logger,
                        "{:?} did a computation with outputs {:?} and inputs {:?} using {:?}",
                        self.site_id_manager.my_site_id,
                        outputs,
                        inputs,
                        compute_asset
                    );
                    for &output_id in outputs.iter() {
                        self.asset_store.insert(output_id, AssetData);
                    }
                    true
                } else {
                    false
                }
            }
        }
    }
}

impl Site {
    fn create_new_asset(&mut self, asset_data: AssetData) -> Result<AssetId, AssetData> {
        match self.inner.site_id_manager.alloc_asset_id() {
            None => Err(asset_data),
            Some(asset_id) => {
                self.inner.asset_store.insert(asset_id, asset_data);
                Ok(asset_id)
            }
        }
    }

    /// Consumes the calling thread
    fn execute(&mut self) {
        loop {
            // remove as many TODO instructions as possible
            let mut i = 0;
            while i < self.todo_instructions.len() {
                let completed = self.inner.try_complete(&mut self.todo_instructions[i]);
                if completed {
                    self.todo_instructions.swap_remove(i);
                    i = 0;
                } else {
                    i += 1;
                }
            }

            // receive a message
            let msg = match self.inner.inbox.recv_timeout(Duration::from_secs(1)) {
                Err(_) => {
                    log!(
                        self.inner.logger,
                        "Site {:?} RECV timeout with\ntodo instructions {:#?}\nassets {:?}",
                        self.inner.site_id_manager.my_site_id,
                        &self.todo_instructions,
                        self.inner.asset_store.keys().collect::<Vec<_>>()
                    );
                    return;
                }
                Ok(msg) => msg,
            };
            match msg {
                Msg::AssetDataRequest { asset_id, requester } => {
                    if let Some(asset_data) = self.inner.asset_store.get(&asset_id) {
                        let msg = Msg::AssetData { asset_id, asset_data: asset_data.clone() };
                        log!(
                            self.inner.logger,
                            "Site {:?} replying to {:?} with msg {:?}",
                            self.inner.site_id_manager.my_site_id,
                            requester,
                            msg
                        );
                        self.inner.peer_outboxes.get(&requester).unwrap().send(msg).unwrap();
                    } else {
                        self.todo_instructions
                            .push(Instruction::SendAssetTo { asset_id, site_id: requester });
                    }
                }
                Msg::AssetData { asset_id, asset_data } => {
                    self.inner.last_requested_at.remove(&asset_id);
                    self.inner.asset_store.insert(asset_id, asset_data);
                }
            }
        }
    }
}

fn setup_network(network_config: NetworkConfig) -> HashMap<SiteId, Site> {
    let mut sites = HashMap::<SiteId, Site>::default();
    let mut outboxes = HashMap::<SiteId, mpsc::Sender<Msg>>::default();
    for (site_id, logger) in network_config.nodes.into_iter() {
        let (outbox, inbox) = mpsc::channel();
        let site = Site {
            todo_instructions: Default::default(),
            inner: SiteInner {
                site_id_manager: SiteIdManager::new(site_id),
                inbox,
                asset_store: Default::default(),
                peer_outboxes: Default::default(),
                last_requested_at: HashMap::default(),
                logger,
            },
        };
        sites.insert(site_id, site);
        outboxes.insert(site_id, outbox);
    }
    let mut add_edge_to = |site_id_from, site_id_to| {
        let outbox = outboxes.get(&site_id_to).unwrap().clone();
        sites.get_mut(&site_id_from).unwrap().inner.peer_outboxes.insert(site_id_to, outbox);
    };
    for &[site_id_a, site_id_b] in network_config.bidir_edges.iter() {
        add_edge_to(site_id_a, site_id_b);
        add_edge_to(site_id_b, site_id_a);
    }
    sites
}

fn main() {
    /*
    example scenario:
    amy has X, bob has Y, cho has F
    bob computes Z=F(X,Y)
    amy gets Z
    */
    const AMY: SiteId = SiteId(0);
    const BOB: SiteId = SiteId(1);
    const CHO: SiteId = SiteId(2);
    let mut sites = setup_network(NetworkConfig {
        nodes: maplit::hashmap! {
            AMY => Box::new(FileLogger::new("./logs/amy.txt")) as Box<_>,
            BOB => Box::new(FileLogger::new("./logs/bob.txt")) as Box<_>,
            CHO => Box::new(FileLogger::new("./logs/cho.txt")) as Box<_>,
        },
        bidir_edges: vec![[AMY, BOB], [BOB, CHO]],
    });
    println!("sites: {:#?}", &sites);
    println!("--------------------------------------");

    // AMY
    let site = sites.get_mut(&AMY).unwrap();
    let aid_x = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 0
    let aid_y = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 1
    let aid_z = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 2
    let aid_f = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 3
    site.inner.asset_store.insert(aid_x, AssetData);
    site.todo_instructions.push(Instruction::AcquireAssetFrom { asset_id: aid_z, site_id: BOB });
    site.todo_instructions.push(Instruction::SendAssetTo { asset_id: aid_x, site_id: BOB });

    // BOB
    let site = sites.get_mut(&BOB).unwrap();
    site.inner.asset_store.insert(aid_y, AssetData);
    site.todo_instructions.push(Instruction::AcquireAssetFrom { asset_id: aid_f, site_id: CHO });
    site.todo_instructions.push(Instruction::ComputeAssetData {
        outputs: vec![aid_z],
        inputs: vec![aid_x, aid_y],
        compute_asset: aid_f,
    });

    // CHO
    let site = sites.get_mut(&CHO).unwrap();
    site.inner.asset_store.insert(aid_f, AssetData);

    crossbeam_utils::thread::scope(|s| {
        for site in sites.values_mut() {
            s.spawn(move |_| site.execute());
        }
    })
    .unwrap();
}
