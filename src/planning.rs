use super::*;

struct SymbolicStore {
    site_has_asset: HashSet<(SiteId, AssetId)>,
    someone_has_asset: HashSet<AssetId>,
}
struct SymbolicProgress {
    computes_todo: HashSet<usize>,
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
    fn new(problem: &Problem) -> Self {
        Self {
            someone_has_asset: problem
                .site_has_asset
                .iter()
                .map(|(_site_id, asset_id)| *asset_id)
                .collect(),
            site_has_asset: problem.site_has_asset.clone(),
        }
    }
    fn insert(&mut self, site_id: SiteId, asset_id: AssetId) {
        self.site_has_asset.insert((site_id, asset_id));
        self.someone_has_asset.insert(asset_id);
    }
}
impl SymbolicProgress {
    fn new(problem: &Problem) -> Self {
        Self { computes_todo: (0..problem.do_compute.len()).collect() }
    }
    fn take_next_possible_compute(
        &mut self,
        problem: &Problem,
        store: &SymbolicStore,
    ) -> Result<usize, Option<usize>> {
        for &i in self.computes_todo.iter() {
            if problem.do_compute[i]
                .needed_assets()
                .all(|asset_id| store.someone_has_asset.contains(asset_id))
            {
                self.computes_todo.remove(&i);
                return Ok(i);
            }
        }
        Err(self.computes_todo.iter().copied().next())
    }
}

pub(crate) fn plan<'a>(
    problem: &'a Problem,
) -> Result<HashMap<SiteId, Vec<Instruction>>, PlanError<'a>> {
    let mut result = HashMap::<SiteId, Vec<Instruction>>::default();
    let mut push_instruction = |site_id: SiteId, ins: Instruction| {
        result.entry(site_id).or_insert_with(Default::default).push(ins);
    };

    let mut symbolic_store = SymbolicStore::new(problem);
    let mut symbolic_progress = SymbolicProgress::new(problem);
    loop {
        match symbolic_progress.take_next_possible_compute(problem, &symbolic_store) {
            Err(None) => return Ok(result),
            Err(Some(index)) => {
                return Err(PlanError::CyclicCausality(&problem.do_compute[index]));
            }
            Ok(index) => {
                // Select a satisfactory site to perform the next ComputeArgs
                let compute_args = &problem.do_compute[index];
                let compute_site = site_for_compute(problem, compute_args)
                    .ok_or(PlanError::NoSiteForCompute(compute_args))?;
                // Generate instructions to route the needed assets to the compute site
                for needed_asset in compute_args.needed_assets() {
                    if !symbolic_store.site_has_asset.contains(&(compute_site, *needed_asset)) {
                        // The compute site DOES NOT have this needed asset yet!
                        // Find a site that does have the asset already
                        // (`compute_sequence` ensures such a site must exist).
                        let having_site = symbolic_store
                            .site_has_asset
                            .iter()
                            .filter_map(asset_filter_mapper(needed_asset))
                            .next()
                            .expect(
                                "`compute_sequence` ensurees SOME site should have this asset!",
                            );
                        symbolic_store.insert(compute_site, *needed_asset);
                        // Tell the haver to send it to the computer, and vice versa
                        // (Having either one of these would suffice).
                        push_instruction(
                            having_site,
                            Instruction::SendAssetTo {
                                asset_id: *needed_asset,
                                site_id: compute_site,
                            },
                        );
                        push_instruction(
                            compute_site,
                            Instruction::AcquireAssetFrom {
                                asset_id: *needed_asset,
                                site_id: having_site,
                            },
                        );
                    }
                }
                // Instruct the compute site to perform the computation itself
                push_instruction(compute_site, Instruction::ComputeAssetData(compute_args.clone()));
                // Update our symbolic store of sites' assets.
                for output_asset in compute_args.outputs.iter() {
                    symbolic_store.insert(compute_site, *output_asset);
                }
            }
        }
    }
}
