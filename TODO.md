# DONE

# TODO
1. embrace network all-to-all reachability and mimic a computer network environment more closely: sites have addresses, and each site simply sends a message to any address. 
1. introduce some notion of cryptographic signature. sites have public and private keypairs. sites identify one another with their public keys. sites sign their network messages.
1. reduce inter-site trust. sites check the signatures of incoming messages. sites don't accept data that they don't want.
1. restructure asset_ids to be `(collection_id, given_name, asset_data_hash)`