use super::*;

pub fn scenario_amy_bob_cho() {
    // Setup the network
    let loggers = vec![
        FileLogger::new("./logs/amy.txt"),
        FileLogger::new("./logs/bob.txt"),
        FileLogger::new("./logs/cho.txt"),
    ];
    let (site_ids, mut sites) = crate::site::new_sites(loggers);
    let [amy, bob, cho]: [SiteId; 3] = std::convert::TryInto::try_into(site_ids).expect("wah");

    // Do the planning
    println!("Site Ids {:?}", [amy, bob, cho]);

    let x = AssetId(0);
    let y = AssetId(1);
    let z = AssetId(2);
    let f = AssetId(3);
    let problem = Problem {
        may_access: maplit::hashset! {
            (amy, x), (bob, x),
            (bob, y),
            (bob, f), (cho, f),
            (cho, z), // TODO check have access to outputs
        },
        may_compute: maplit::hashset! { (bob, f) },
        site_has_asset: maplit::hashset! { (amy, x), (bob, y) , (cho, f)  },
        do_compute: vec![ComputeArgs { inputs: vec![x, y], outputs: vec![z], compute_asset: f }],
    };
    let planned = planning::plan(&problem).unwrap();
    println!("planned: {:#?}\n------------------", &planned);

    // setup the network
    println!("sites: {:#?}", &sites);
    println!("--------------------------------------");

    // give the sites their planned instructions
    for (site_id, instructions) in planned {
        sites.get_mut(&site_id).unwrap().todo_instructions.extend(instructions)
    }

    // give them their initial data
    sites.get_mut(&amy).unwrap().inner.asset_store.insert(x, AssetData { bits: 0xDEADBEEF });
    sites.get_mut(&bob).unwrap().inner.asset_store.insert(y, AssetData { bits: 0xD00DEEDADA });
    sites.get_mut(&cho).unwrap().inner.asset_store.insert(f, AssetData { bits: 0xC0FEFE });

    // run the system
    crossbeam_utils::thread::scope(|s| {
        for site in sites.values_mut() {
            s.spawn(move |_| site.execute());
        }
    })
    .unwrap();
}
