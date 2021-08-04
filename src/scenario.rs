use super::*;

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

pub fn scenario_amy_bob_cho() {
    // Do the planning
    let mut allocator = SiteIdManager::new(SiteId(42));
    let x = allocator.alloc_asset_id().unwrap();
    let y = allocator.alloc_asset_id().unwrap();
    let z = allocator.alloc_asset_id().unwrap();
    let f = allocator.alloc_asset_id().unwrap();
    let problem = Problem {
        may_access: maplit::hashset! {
            (SiteId(0), x), (SiteId(1), x),
            (SiteId(1), y),
            (SiteId(1), f), (SiteId(2), f),
            (SiteId(2), z), // TODO check have access to outputs
        },
        may_compute: maplit::hashset! { (SiteId(1), f) },
        site_has_asset: maplit::hashset! { (SiteId(0), x), (SiteId(1), y) , (SiteId(2), f)  },
        do_compute: vec![ComputeArgs { inputs: vec![x, y], outputs: vec![z], compute_asset: f }],
    };
    let planned = planning::plan(&problem).unwrap();
    println!("planned: {:#?}\n------------------", &planned);

    // setup the network
    const AMY: SiteId = SiteId(0);
    const BOB: SiteId = SiteId(1);
    const CHO: SiteId = SiteId(2);
    let mut sites = setup_network(NetworkConfig {
        nodes: maplit::hashmap! {
            AMY => FileLogger::new("./logs/amy.txt"),
            BOB => FileLogger::new("./logs/bob.txt"),
            CHO => FileLogger::new("./logs/cho.txt"),
        },
        bidir_edges: vec![[AMY, BOB], [BOB, CHO]],
    });
    println!("sites: {:#?}", &sites);
    println!("--------------------------------------");

    // give the sites their planned instructions
    for (site_id, instructions) in planned {
        sites.get_mut(&site_id).unwrap().todo_instructions.extend(instructions)
    }

    // give them their initial data
    sites.get_mut(&AMY).unwrap().inner.asset_store.insert(x, AssetData);
    sites.get_mut(&BOB).unwrap().inner.asset_store.insert(y, AssetData);
    sites.get_mut(&CHO).unwrap().inner.asset_store.insert(f, AssetData);

    // run the system
    crossbeam_utils::thread::scope(|s| {
        for site in sites.values_mut() {
            s.spawn(move |_| site.execute());
        }
    })
    .unwrap();
}

pub fn amy_bob_cho() {
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
            AMY => FileLogger::new("./logs/amy.txt"),
            BOB => FileLogger::new("./logs/bob.txt"),
            CHO => FileLogger::new("./logs/cho.txt"),
        },
        bidir_edges: vec![[AMY, BOB], [BOB, CHO]],
    });
    println!("sites: {:#?}", &sites);
    println!("--------------------------------------");

    // AMY
    let site = sites.get_mut(&AMY).unwrap();
    // AMY allocates the identifiers for all assets, present and future (I, the planner, am using her allocator)
    let aid_x = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 0,0
    let aid_y = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 0,1
    let aid_z = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 0,2
    let aid_f = site.inner.site_id_manager.alloc_asset_id().unwrap(); // 0,3
                                                                      // "create" dataset X at AMY (dummy data)
    site.inner.asset_store.insert(aid_x, AssetData);
    // "create" dataset X at AMY (dummy data)
    site.todo_instructions.push(Instruction::AcquireAssetFrom { asset_id: aid_z, site_id: BOB });
    site.todo_instructions.push(Instruction::SendAssetTo { asset_id: aid_x, site_id: BOB });

    // BOB
    let site = sites.get_mut(&BOB).unwrap();
    site.inner.asset_store.insert(aid_y, AssetData);
    site.todo_instructions.push(Instruction::AcquireAssetFrom { asset_id: aid_f, site_id: CHO });
    site.todo_instructions.push(Instruction::ComputeAssetData(ComputeArgs {
        outputs: vec![aid_z],
        inputs: vec![aid_x, aid_y],
        compute_asset: aid_f,
    }));

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
