

def cache_add_selection(node_metadata, cache_method):
    if cache_method == "adaptive":
        # TODO: implement the selection for real
        # For now: cache if the vid is: binarise(26), recons3D(9), skeletonize(35), segment_red(34)
        vid = node_metadata['vid']
        if (vid == 26) or (vid == 9) or (vid == 35) or (vid == 34):
            return True 
        else:
            return False

    if cache_method == "greedy":
        return True
    
    if cache_method == "None":
        return False
    # retrun False if cache method is unknown
    return False

def cache_reuse_selection(node_metadata, reuse_method):
    if reuse_method == "None":
        return False
    if reuse_method == "greedy":
        return True
    if reuse_method == "adaptive":
        # TODO: implement the selection for real
        # For now: cache if the vid is: binarise(26), recons3D(9), skeletonize(35), segment_red(34)
        vid = node_metadata['vid']
        if (vid == 26) or (vid == 9) or (vid == 35) or (vid == 34):
            return True 
        else:
            return False
    else:
        return False

def get_cache_site(cache_site_method, current_site, data_metadata, cloudsite):
    # Check if at least one site has storage :
    sites = cloudsite.get_site_available(storage_to_store=data_metadata['size_output'])
    if not sites:
        return False        

    # put data on the closer site to the data generated
    if cache_site_method == "transfer":
        lsites={}
        for s in sites:
            lsites[s.site] = s.storage_available
            return max(lsites, key=lsites.get)
        # get a dict of transfer rate of site with enough available storage
        r = cloudsite.get_transfer_rate(site=current_site)
        tr = json.loads(r[0].transfer_rate)
        d={k:tr[k] for k in lsites.keys() if k in tr}
        # if the site where the data has been generated is available - use it
        if current_site in d:
            return current_site
        # otherwise use the one with fastest transmission rate
        else:
            return max(d, key=d.get)
        
    # put data on the site that has the most available storage
    if cache_site_method == "max_storage":
        lsites={}
        for s in sites:
            lsites[s.site] = s.storage_available
            return max(lsites, key=lsites.get)
        return max(lsites, key=lsites.get)

    if cache_site_method == "min_cost":
        lsites={}
        for s in sites:
            lsites[s.site] = s.cost_storage
            return min(lsites, key=lsites.get)

    else:
        return False