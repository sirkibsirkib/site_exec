use super::*;

fn setup_network(network_config: NetworkConfig) -> HashMap<SiteId, Site> {
    let mut sites = HashMap::<SiteId, Site>::default();
    let mut outboxes = HashMap::<SiteId, mpsc::Sender<Msg>>::default();
    for (site_id, logger) in network_config.nodes.into_iter() {
        let (outbox, inbox) = mpsc::channel();
        let site = Site {
            todo_instructions: Default::default(),
            inner: SiteInner {
                site_id,
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
    let x = AssetId(0);
    let y = AssetId(1);
    let z = AssetId(2);
    let f = AssetId(3);
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
    sites.get_mut(&AMY).unwrap().inner.asset_store.insert(x, AssetData { bits: 0xDEADBEEF });
    sites.get_mut(&BOB).unwrap().inner.asset_store.insert(y, AssetData { bits: 0xD00DEEDADA });
    sites.get_mut(&CHO).unwrap().inner.asset_store.insert(f, AssetData { bits: 0xC0FEFE });

    // run the system
    crossbeam_utils::thread::scope(|s| {
        for site in sites.values_mut() {
            s.spawn(move |_| site.execute());
        }
    })
    .unwrap();
}
