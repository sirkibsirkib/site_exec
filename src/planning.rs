use super::*;

struct SymbolicStore {
    site_has_asset: HashSet<(SiteId, AssetId)>,
    someone_has_asset: HashSet<AssetId>,
}
struct SymbolicProgress<'a> {
    computes_todo: Vec<&'a ComputeArgs>,
}
//////////////////

fn asset_filter_mapper(
    filter_asset: &AssetId,
) -> impl Fn(&(SiteId, AssetId)) -> Option<SiteId> + '_ {
    move |(site_id, asset_id)| {
        if asset_id == filter_asset {
            Some(*site_id)
        } else {
            None
        }
    }
}

fn site_for_compute(problem: &Problem, compute_args: &ComputeArgs) -> Option<SiteId> {
    // assuming all-pairs site reachability. A site is eligible to compute iff...
    // ... (a) it is permitted to use the given asset as compute, and ...
    let sites_that_may_compute =
        problem.may_compute.iter().filter_map(asset_filter_mapper(&compute_args.compute_asset));
    // ... (b) it is permitted to access all needed assets.
    let mut sites_that_may_also_access = sites_that_may_compute.filter(|site_id| {
        compute_args
            .needed_assets()
            .all(|needed_asset| problem.may_access.contains(&(*site_id, *needed_asset)))
    });
    // We select the first satisfactory site
    sites_that_may_also_access.next()
}

impl<'a> SymbolicStore {
    fn with_assets(site_has_asset: &HashSet<(SiteId, AssetId)>) -> Self {
        Self {
            someone_has_asset: site_has_asset
                .iter()
                .map(|(_site_id, asset_id)| *asset_id)
                .collect(),
            site_has_asset: site_has_asset.clone(),
        }
    }
    fn insert(&mut self, site_id: SiteId, asset_id: AssetId) {
        self.site_has_asset.insert((site_id, asset_id));
        self.someone_has_asset.insert(asset_id);
    }
}
impl<'a> SymbolicProgress<'a> {
    fn with_compute_to_do(iter: impl Iterator<Item = &'a ComputeArgs>) -> Self {
        Self { computes_todo: iter.collect() }
    }
    fn take_feasible_compute<'b>(
        &'b mut self,
        store: &'b SymbolicStore,
    ) -> Result<&'a ComputeArgs, Option<&'a ComputeArgs>> {
        // "feasible" means that all input assets are available
        for (i, compute_args) in self.computes_todo.iter().enumerate() {
            if compute_args
                .needed_assets()
                .all(|asset_id| store.someone_has_asset.contains(asset_id))
            {
                return Ok(self.computes_todo.remove(i));
            }
        }
        Err(self.computes_todo.iter().copied().next())
    }
}

/// Compute a set of instructions to plan for a set of sites, for the given problem
pub(crate) fn plan<'a>(
    problem: &'a Problem,
) -> Result<HashMap<SiteId, Vec<Instruction>>, PlanError<'a>> {
    // `instructions` is incrementally populated before being ultimately returned.
    // We symbolically execute
    let mut instructions = HashMap::<SiteId, Vec<Instruction>>::default();
    let mut push_instruction = |site_id: SiteId, ins: Instruction| {
        instructions.entry(site_id).or_insert_with(Default::default).push(ins);
    };
    // Our symbolic execution starts with an initial state where...
    // ... sites' initial asset storage is given by the problem spec, and
    let mut symbolic_store = SymbolicStore::with_assets(&problem.site_has_asset);
    // ... all compute tasks in the problem spec remain to be done.
    let mut symbolic_progress = SymbolicProgress::with_compute_to_do(problem.do_compute.iter());
    loop {
        // Select the next compute task to do
        match symbolic_progress.take_feasible_compute(&symbolic_store) {
            Err(remaining_compute) => {
                // Stop! There is no more progress possible because...
                return match remaining_compute {
                    None => Ok(instructions), // ... we completed all the compute steps
                    Some(remaining_compute) => {
                        // ... we found an example of a compute task we cannot complete
                        Err(PlanError::CyclicCausality(remaining_compute))
                    }
                };
            }
            Ok(next_compute) => {
                // Symbolically execute `next_compute`.
                // Find a feasible site to complete the computation instruction
                let compute_site = site_for_compute(problem, next_compute)
                    .ok_or(PlanError::NoSiteForCompute(next_compute))?;
                push_instruction(compute_site, Instruction::ComputeAssetData(next_compute.clone()));
                // Route the instruction's input assets to `compute_site` as necessary.
                for needed_asset in next_compute.needed_assets() {
                    if symbolic_store.site_has_asset.contains(&(compute_site, *needed_asset)) {
                        // This asset is already present at the compute site.
                        continue;
                    }
                    // The compute site DOES NOT have this needed asset yet!
                    // Find a site that does have the asset already
                    // (`take_feasible_compute` ensures such a site must exist).
                    let having_site = symbolic_store
                        .site_has_asset
                        .iter()
                        .filter_map(asset_filter_mapper(needed_asset))
                        .next()
                        .expect("`compute_sequence` ensurees SOME site should have this asset!");
                    symbolic_store.insert(compute_site, *needed_asset);
                    // Tell sender and receiver sites to send and receive respectively.
                    // (Including either of these would suffice)
                    push_instruction(
                        having_site,
                        Instruction::SendAssetTo { asset_id: *needed_asset, site_id: compute_site },
                    );
                    push_instruction(
                        compute_site,
                        Instruction::AcquireAssetFrom {
                            asset_id: *needed_asset,
                            site_id: having_site,
                        },
                    );
                }
                // Update our symbolic store of sites' assets.
                for output_asset in next_compute.outputs.iter() {
                    symbolic_store.insert(compute_site, *output_asset);
                }
            }
        }
    }
}
