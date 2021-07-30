use super::*;

enum InstructionResult {
    NoProgress,
    RemoveThis,
    RemoveThisAndRestart,
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
        log!(
            self.logger,
            "{:?} sending to {:?} msg {:?}",
            self.site_id_manager.my_site_id,
            dest_id,
            &msg
        );
        self.peer_outboxes.get(&dest_id).unwrap().send(msg).unwrap();
    }
    fn try_complete(&mut self, instruction: &mut Instruction) -> InstructionResult {
        match instruction {
            Instruction::AcquireAssetFrom { asset_id, site_id } => {
                if self.asset_store.contains_key(asset_id) {
                    return InstructionResult::RemoveThis;
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
                InstructionResult::NoProgress
            }
            Instruction::SendAssetTo { asset_id, site_id } => {
                if let Some(asset_data) = self.asset_store.get(&asset_id) {
                    let msg =
                        Msg::AssetData { asset_id: *asset_id, asset_data: asset_data.clone() };
                    self.send_to(*site_id, msg);
                    InstructionResult::RemoveThis
                } else {
                    InstructionResult::NoProgress
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
                    InstructionResult::RemoveThisAndRestart
                } else {
                    InstructionResult::NoProgress
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
        loop {
            // remove as many TODO instructions as possible
            let mut i = 0;
            while i < self.todo_instructions.len() {
                let result = self.inner.try_complete(&mut self.todo_instructions[i]);
                match result {
                    InstructionResult::NoProgress => i += 1,
                    InstructionResult::RemoveThis => {
                        self.todo_instructions.swap_remove(i);
                    }
                    InstructionResult::RemoveThisAndRestart => {
                        self.todo_instructions.swap_remove(i);
                        i = 0;
                    }
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
                        self.inner.send_to(requester, msg);
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
