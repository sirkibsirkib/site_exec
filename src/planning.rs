use super::*;

// struct Concretizer<'a, A: AssetIdAllocator> {
//     allocator: &'a mut A,
//     asset_name_to_id: HashMap<AssetIndex, AssetId>,
// }
// impl<'a, A: AssetIdAllocator> Concretizer<'a, A> {
//     fn try_concretize_name(&mut self, asset_name: &AssetName) -> Option<AssetId> {
//         Some(match asset_name {
//             AssetName::Concrete { asset_id } => *asset_id,
//             AssetName::Abstract { index } => {
//                 if let Some(asset_id) = self.asset_name_to_id.get(index) {
//                     *asset_id
//                 } else {
//                     let asset_id = self.allocator.alloc_asset_id()?; // may return None
//                     self.asset_name_to_id.insert(*index, asset_id);
//                     asset_id
//                 }
//             }
//         })
//     }
//     fn tr
//     fn try_concretize_instruction(
//         &mut self,
//         instruction: &Instruction<AssetName>,
//     ) -> Option<Instruction<AssetId>> {
//         Some(match instruction {

//     Instruction::SendAssetTo { asset_id, site_id } => ,
//     Instruction::AcquireAssetFrom { asset_id, site_id },
//     Instruction::ComputeAssetData(compute_args),
//         })
//     }
// }

fn site_for_computes<'a, 'b>(
    problem: &'a Problem,
    computed_order: &'b [&'a ComputeArgs],
) -> Result<Vec<SiteId>, &'a ComputeArgs> {
    computed_order
        .iter()
        .copied()
        .map(|compute_args: &ComputeArgs| {
            println!("... considering {:?}", compute_args);
            let sites_that_may_compute =
                problem.may_compute.iter().filter_map(|(site_id, asset_id)| {
                    if asset_id == &compute_args.compute_asset {
                        Some(*site_id)
                    } else {
                        None
                    }
                });
            let mut sites_that_may_also_access = sites_that_may_compute.filter(|site_id| {
                compute_args
                    .needed_assets()
                    .all(|needed_asset| problem.may_access.contains(&(*site_id, *needed_asset)))
            });
            sites_that_may_also_access.next().ok_or(compute_args)
        })
        .collect()
}

fn compute_order(problem: &Problem) -> Result<Vec<&ComputeArgs>, &ComputeArgs> {
    // Symbolic execution of ComputeAssetData instructions. If we get stuck -> planning impossible.
    let mut computed_order = Vec::with_capacity(problem.do_compute.len());
    let mut someone_has_asset: HashSet<AssetId> =
        problem.site_has_asset.iter().map(|(_site_id, asset_id)| *asset_id).collect();
    let mut compute_todo: Vec<&ComputeArgs> = problem.do_compute.iter().collect();
    let mut i = 0;
    while i < compute_todo.len() {
        let compute_args = &compute_todo[i];
        if compute_args.needed_assets().all(|asset_id| someone_has_asset.contains(asset_id)) {
            someone_has_asset.extend(compute_args.outputs.iter().copied());
            computed_order.push(compute_todo.swap_remove(i));
        } else {
            i += 1;
        }
    }
    compute_todo.iter().copied().next().ok_or(computed_order).map_or_else(Ok, Err)
}

pub(crate) fn plan<'a>(
    problem: &'a Problem,
    allocator: &mut impl AssetIdAllocator,
) -> Result<HashMap<Site, Vec<Instruction>>, PlanError<'a>> {
    let computed_order = compute_order(problem).map_err(PlanError::CyclicCausality)?;
    let site_order =
        site_for_computes(problem, &computed_order).map_err(PlanError::NoSiteForCompute)?;
    println!("{:?}", site_order);
    Ok(Default::default())
    // Symbolic execution of ComputeAssetData instructions. If we get stuck -> planning impossible.
    // let mut site_has_asset = problem.site_has_asset.clone();
    // let mut someone_has_asset: HashSet<AssetId> =
    //     site_has_asset.iter().map(|(site_id, asset_id)| *asset_id).collect();
    // let mut ret: HashMap<Site, Vec<Instruction>> = Default::default();
    // let mut compute_todo: Vec<&'a ComputeArgs> = problem.do_compute.iter().collect();
    // let mut i = 0;
    // let mut compute_order: Vec<&'a ComputeArgs> = Vec::with_capacity();
    // while i < compute_todo.len() {
    //     let compute_args = &compute_todo[i];
    //     if compute_args.needed_assets().all(|asset_id| someone_has_asset.contains(asset_id)) {
    //         //
    //         asset_store.extend(compute_args.outputs.iter().copied());
    //         let compute_args = compute_todo.swap_remove(i);
    //         // where to compute it?
    //         let site_at = problem.may_compute.iter().find_map(|(site_id, asset_id)| {
    //             if *asset_id == compute_args.compute_asset
    //                 && compute_args
    //                     .needed_assets()
    //                     .all(|asset_id| problem.may_access.contains(&(*site_id, *asset_id)))
    //             {
    //                 Some(site_id)
    //             } else {
    //                 None
    //             }
    //         });
    //         if let Some(site_id) = site_at {
    //             compute_at.push((site_id, compute_args));
    //         } else {
    //             return Err(PlanError::NoSiteForCompute(compute_args));
    //         }
    //     } else {
    //         i += 1;
    //     }
    // }
    // // No next ComputeAssetData instruction is possible
    // if let Some(compute_args) = compute_todo.first() {
    //     return Err(PlanError::NoWayToAcquireInputsForCompute(compute_args));
    // }
    // // ret
    // todo!()
}

pub(crate) fn plan2<'a>(
    problem: &'a Problem,
) -> Result<HashMap<Site, Vec<Instruction>>, PlanError<'a>> {
    todo!()
}
