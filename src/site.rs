use super::*;

enum InsExecResult {
    Incomplete,
    Complete { added_assets_to_store: bool },
}

impl SiteIdManager {
    pub fn new(my_site_id: SiteId) -> Self {
        Self {
            my_site_id,
            asset_index_list: Default::default(),
            asset_index_seq_head: Some(AssetIndex(0)),
        }
    }
    pub fn alloc_asset_id(&mut self) -> Option<AssetId> {
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
    pub fn try_free_asset_id(&mut self, asset_id: AssetId) -> bool {
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
        log!(self.logger, "Sending to {:?} msg {:?}", dest_id, &msg);
        self.peer_outboxes.get(&dest_id).unwrap().send(msg).unwrap();
    }
    fn try_complete(&mut self, instruction: &mut Instruction<AssetId>) -> InsExecResult {
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
                    let msg = Msg::AssetDataRequest {
                        asset_id: *asset_id,
                        requester: self.site_id_manager.my_site_id,
                    };
                    self.send_to(*site_id, msg);
                }
                InsExecResult::Incomplete
            }
            Instruction::SendAssetTo { asset_id, site_id } => {
                if let Some(asset_data) = self.asset_store.get(&asset_id) {
                    let msg =
                        Msg::AssetData { asset_id: *asset_id, asset_data: asset_data.clone() };
                    self.send_to(*site_id, msg);
                    InsExecResult::Complete { added_assets_to_store: false }
                } else {
                    InsExecResult::Incomplete
                }
            }
            Instruction::ComputeAssetData(parameterized_compute) => {
                if parameterized_compute
                    .needed_assets()
                    .all(|asset_id| self.asset_store.contains_key(&asset_id))
                {
                    log!(self.logger, "Did a computation with {:?} ", &parameterized_compute);
                    for &output_id in parameterized_compute.outputs.iter() {
                        self.asset_store.insert(output_id, AssetData);
                    }
                    InsExecResult::Complete { added_assets_to_store: true }
                } else {
                    InsExecResult::Incomplete
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
    pub fn execute(&mut self) {
        let start = Instant::now();

        log!(
            self.inner.logger,
            "Started executing at {:?}. My site_id is {:?}",
            &start,
            self.inner.site_id_manager.my_site_id,
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
                let msg = match self.inner.inbox.recv_timeout(Duration::from_secs(1)) {
                    Ok(msg) => msg,
                    Err(_) => {
                        log!(
                            self.inner.logger,
                            "RECV timeout with todo instructions {:#?} assets {:?}",
                            &self.todo_instructions,
                            self.inner.asset_store.keys()
                        );
                        return;
                    }
                };
                log!(self.inner.logger, "Received msg {:?}", msg);
                match msg {
                    Msg::AssetDataRequest { asset_id, requester } => {
                        if let Some(asset_data) = self.inner.asset_store.get(&asset_id) {
                            let msg = Msg::AssetData { asset_id, asset_data: asset_data.clone() };
                            self.inner.send_to(requester, msg);
                        } else {
                            self.todo_instructions
                                .push(Instruction::SendAssetTo { asset_id, site_id: requester });
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
