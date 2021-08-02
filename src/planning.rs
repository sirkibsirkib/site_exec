use super::*;

pub(crate) enum PlanError<'a> {
    NoWayToAcquireInputsForCompute(&'a ParameterizedCompute),
    NoSiteForCompute(&'a ParameterizedCompute),
}

pub(crate) fn plan(problem: &Problem) -> Result<HashMap<Site, Vec<Instruction>>, PlanError> {
    // NOTE: define
    let mut unrached_compute: Vec<&ParameterizedCompute> = problem.do_compute.iter().collect();
    let mut compute_at = Vec::with_capacity(unrached_compute.len());
    let mut got_assets: HashSet<AssetId> = problem.assets_at_sites.keys().cloned().collect();

    let mut i = 0;
    while i < unrached_compute.len() {
        let parameterized_compute = &unrached_compute[i];
        if parameterized_compute.needed_assets().all(|asset_id| got_assets.contains(asset_id)) {
            got_assets.extend(parameterized_compute.outputs.iter().copied());
            let parameterized_compute = unrached_compute.swap_remove(i);
            // where to compute it?
            let site_at = problem.may_compute.iter().find_map(|(site_id, asset_id)| {
                if *asset_id == parameterized_compute.compute_asset {
                    Some(site_id)
                } else {
                    None
                }
            });
            if let Some(site_id) = site_at {
                compute_at.push((site_id, parameterized_compute));
            } else {
                return Err(PlanError::NoSiteForCompute(parameterized_compute));
            }
        } else {
            i += 1;
        }
    }
    if let Some(parameterized_compute) = unrached_compute.first() {
        return Err(PlanError::NoWayToAcquireInputsForCompute(parameterized_compute));
    }
    println!("{:#?}", compute_at);
    todo!()
}
