use super::*;

enum InsExecResult {
    Incomplete,
    Complete { added_assets_to_store: bool },
}

pub(crate) fn new_sites(loggers: Vec<Box<dyn Logger>>) -> (Vec<SiteId>, HashMap<SiteId, Site>) {
    struct Parts {
        inbox: Receiver<SignedMsg>,
        logger: Box<dyn Logger>,
        keypair: Keypair,
    }
    let mut outboxes = HashMap::default();
    let mut parts = vec![];
    let mut site_ids = vec![];
    for logger in loggers {
        let keypair = Keypair::generate(&mut rand_core::OsRng);
        let (outbox, inbox) = crossbeam_channel::unbounded();
        let site_id = *SiteId::from_public_key_ref(&keypair.public);

        outboxes.insert(site_id, outbox);
        site_ids.push(site_id);
        parts.push(Parts { inbox, logger, keypair });
    }
    let outboxes = Arc::new(outboxes);
    let sites = parts
        .into_iter()
        .map(|Parts { inbox, logger, keypair }| {
            let site_id = *SiteId::from_public_key_ref(&keypair.public);
            let site = Site {
                inner: SiteInner {
                    keypair,
                    logger,
                    outboxes: outboxes.clone(),
                    inbox,
                    last_requested_at: Default::default(),
                    asset_store: Default::default(),
                },
                todo_instructions: Default::default(), // todo
            };
            (site_id, site)
        })
        .collect();
    (site_ids, sites)
}

fn actual_compute(
    store: &HashMap<AssetId, AssetData>,
    compute_args: &ComputeArgs,
) -> Option<HashMap<AssetId, AssetData>> {
    let mut hasher = fnv::FnvHasher::default();
    use std::hash::Hasher;
    for needed_asset in compute_args.needed_assets() {
        hasher.write_u64(store.get(needed_asset)?.bits);
    }
    Some(
        compute_args
            .outputs
            .iter()
            .map(|&output_asset_id| {
                let data = AssetData { bits: hasher.finish() };
                hasher.write_u64(data.bits);
                (output_asset_id, data)
            })
            .collect(),
    )
}

impl SiteInner {
    const REQUEST_PERIOD: Duration = Duration::from_millis(300);

    fn send_to(&mut self, dest_id: &SiteId, msg: Msg) {
        log!(self.logger, "Sending to {:?} msg {:?}", dest_id, &msg);
        self.outboxes.get(dest_id).unwrap().send(msg.sign(&self.keypair)).unwrap();
    }
    fn try_complete(&mut self, instruction: &mut Instruction) -> InsExecResult {
        match instruction {
            Instruction::AcquireAssetFrom { asset_id, site_id } => {
                if self.asset_store.contains_key(asset_id) {
                    return InsExecResult::Complete { added_assets_to_store: false };
                }
                let now = Instant::now();
                let recent_request = self
                    .last_requested_at
                    .get(asset_id)
                    .map(|&at| now - at < Self::REQUEST_PERIOD)
                    .unwrap_or(false);
                if !recent_request {
                    // Did not recently request this asset! Do so!
                    self.last_requested_at.insert(*asset_id, now);
                    let msg = Msg::AssetDataRequest { asset_id: *asset_id };
                    self.send_to(site_id, msg);
                }
                InsExecResult::Incomplete
            }
            Instruction::SendAssetTo { asset_id, site_id } => {
                if let Some(asset_data) = self.asset_store.get(&asset_id) {
                    let msg =
                        Msg::AssetData { asset_id: *asset_id, asset_data: asset_data.clone() };
                    self.send_to(site_id, msg);
                    InsExecResult::Complete { added_assets_to_store: false }
                } else {
                    InsExecResult::Incomplete
                }
            }
            Instruction::ComputeAssetData(compute_args) => {
                if compute_args
                    .needed_assets()
                    .all(|asset_id| self.asset_store.contains_key(&asset_id))
                {
                    log!(self.logger, "Did a computation with {:?} ", &compute_args);
                    self.asset_store.extend(
                        actual_compute(&self.asset_store, compute_args).expect("compute failed!"),
                    );
                    InsExecResult::Complete { added_assets_to_store: true }
                } else {
                    InsExecResult::Incomplete
                }
            }
        }
    }
}

impl Site {
    /// Consumes the calling thread
    pub fn execute(&mut self) {
        let start = Instant::now();
        log!(
            self.inner.logger,
            "Started executing at {:?}. My site_id is {:?}",
            &start,
            SiteId::from_public_key_ref(&self.inner.keypair.public),
        );
        'execute_loop: loop {
            // Any instruction might be completable!

            let mut i = 0;
            // loop invariant: todo instructions with indices in [0..i)] would return InsExecResult::Incomplete if checked with `try_complete`.
            while i < self.todo_instructions.len() {
                let result = self.inner.try_complete(&mut self.todo_instructions[i]);
                match result {
                    InsExecResult::Incomplete => {
                        // retain this instruction, consider the next
                        i += 1;
                    }
                    InsExecResult::Complete { added_assets_to_store: false } => {
                        // remove this instruction, consider all subsequent instructions
                        self.todo_instructions.swap_remove(i);
                    }
                    InsExecResult::Complete { added_assets_to_store: true } => {
                        // remove this instruction, consider all instructions
                        self.todo_instructions.swap_remove(i);
                        continue 'execute_loop;
                    }
                }
            }
            // No instructions are completable.

            if self.todo_instructions.is_empty() {
                log!(self.inner.logger, "Ran out of TODO instructions after {:?}", start.elapsed());
            }

            // receive 1+ messages until we have further populated the asset store
            loop {
                let signed_msg = match self.inner.inbox.recv_timeout(Duration::from_secs(1)) {
                    Ok(signed_msg) => signed_msg,
                    Err(_) => {
                        log!(
                            self.inner.logger,
                            "RECV timeout with todo instructions {:#?} assets {:?}",
                            &self.todo_instructions,
                            &self.inner.asset_store
                        );
                        return;
                    }
                };
                if let Err(e) = signed_msg.verify() {
                    log!(self.inner.logger, "Msg verification failed {:?} {:?}", &signed_msg, e);
                    continue;
                }
                log!(self.inner.logger, "Received verfied msg {:?}", &signed_msg.msg);
                match signed_msg.msg {
                    Msg::AssetDataRequest { asset_id } => {
                        if let Some(asset_data) = self.inner.asset_store.get(&asset_id) {
                            let msg = Msg::AssetData { asset_id, asset_data: asset_data.clone() };
                            self.inner.send_to(signed_msg.sender(), msg);
                        } else {
                            self.todo_instructions.push(Instruction::SendAssetTo {
                                asset_id,
                                site_id: *signed_msg.sender(),
                            });
                        }
                    }
                    Msg::AssetData { asset_id, asset_data } => {
                        self.inner.last_requested_at.remove(&asset_id);
                        self.inner.asset_store.insert(asset_id, asset_data);
                        continue 'execute_loop;
                    }
                }
            }
        }
    }
}
