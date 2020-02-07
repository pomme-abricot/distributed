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