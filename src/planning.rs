use super::*;

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

fn sequenced_compute(problem: &Problem) -> Result<Vec<&ComputeArgs>, &ComputeArgs> {
    // Sequential symbolic execution of ComputeArgs at a global view (no notion of site).
    // We discover (and return) a sequence of
    let mut compute_sequence = Vec::with_capacity(problem.do_compute.len());
    let mut someone_has_asset: HashSet<AssetId> =
        problem.site_has_asset.iter().map(|(_site_id, asset_id)| *asset_id).collect();
    let mut compute_todo: Vec<&ComputeArgs> = problem.do_compute.iter().collect();
    let mut i = 0;
    while i < compute_todo.len() {
        let compute_args = &compute_todo[i];
        if compute_args.needed_assets().all(|asset_id| someone_has_asset.contains(asset_id)) {
            someone_has_asset.extend(compute_args.outputs.iter().copied());
            compute_sequence.push(compute_todo.swap_remove(i));
        } else {
            i += 1;
        }
    }
    compute_todo.iter().copied().next().ok_or(compute_sequence).map_or_else(Ok, Err)
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

struct SymbolicAssetStore {
    site_has_asset: HashSet<(SiteId, AssetId)>,
    someone_has_asset: HashSet<AssetId>,
}
impl SymbolicAssetStore {
    fn new(site_has_asset: HashSet<(SiteId, AssetId)>) -> Self {
        Self {
            someone_has_asset: site_has_asset
                .iter()
                .map(|(_site_id, asset_id)| *asset_id)
                .collect(),
            site_has_asset,
        }
    }
    fn insert(&mut self, site_id: SiteId, asset_id: AssetId) {
        self.site_has_asset.insert((site_id, asset_id));
        self.someone_has_asset.insert(asset_id);
    }
}

pub(crate) fn plan<'a>(
    problem: &'a Problem,
) -> Result<HashMap<SiteId, Vec<Instruction>>, PlanError<'a>> {
    // Sequence ComputeArgs s.t. for all indices i, compute_sequence[i]
    // is causally dependent on no ComputeArgs in compute_sequence[i..].
    // This step is necessary for two reasons:
    // 1. failing at this step catches an erroneous cyclic causal dependency
    // 2. the sequences drive the symbolic execution up next.
    let compute_sequence: Vec<&ComputeArgs> =
        sequenced_compute(problem).map_err(PlanError::CyclicCausality)?;
    // Sequential symbolic execution of compute steps in serialized order,
    // keeping track of which site has which assets.
    let mut result = HashMap::<SiteId, Vec<Instruction>>::default();
    let mut push_instruction = |site_id: SiteId, ins: Instruction| {
        result.entry(site_id).or_insert_with(Default::default).push(ins);
    };
    let mut symbolic_asset_store = SymbolicAssetStore::new(problem.site_has_asset.clone());
    for compute_args in compute_sequence {
        // Select a satisfactory site to perform the next ComputeArgs
        let compute_site = site_for_compute(problem, compute_args)
            .ok_or(PlanError::NoSiteForCompute(compute_args))?;
        // Generate instructions to route the needed assets to the compute site
        for needed_asset in compute_args.needed_assets() {
            if !symbolic_asset_store.site_has_asset.contains(&(compute_site, *needed_asset)) {
                // The compute site DOES NOT have this needed asset yet!
                // Find a site that does have the asset already
                // (`compute_sequence` ensures such a site must exist).
                let having_site = symbolic_asset_store
                    .site_has_asset
                    .iter()
                    .filter_map(asset_filter_mapper(needed_asset))
                    .next()
                    .expect("`compute_sequence` ensurees SOME site should have this asset!");
                symbolic_asset_store.insert(compute_site, *needed_asset);
                // Tell the haver to send it to the computer, and vice versa
                // (Having either one of these would suffice).
                push_instruction(
                    having_site,
                    Instruction::SendAssetTo { asset_id: *needed_asset, site_id: compute_site },
                );
                push_instruction(
                    compute_site,
                    Instruction::AcquireAssetFrom { asset_id: *needed_asset, site_id: having_site },
                );
            }
        }
        // Instruct the compute site to perform the computation itself
        push_instruction(compute_site, Instruction::ComputeAssetData(compute_args.clone()));
        // Update our symbolic store of sites' assets.
        for output_asset in compute_args.outputs.iter() {
            symbolic_asset_store.insert(compute_site, *output_asset);
        }
    }
    Ok(result)
}
