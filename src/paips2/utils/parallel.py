import ray

def paralellize_fn(fn, data, n_workers, fn_args=[], fn_kwargs={}):
    ray_initted = False
    if not ray.is_initialized():
        ray.init(num_cpus=n_workers)
    else:
        ray_initted = True
    remote_fn = ray.remote(fn)
    elems_per_worker = len(data)//n_workers
    if isinstance(data,list):
        data_per_worker = [data[i:i+elems_per_worker] for i in range(0,len(data),elems_per_worker)]
        results = ray.get([remote_fn.remote(data_i) for data_i in data_per_worker])
        output = sum(results,[])
    if not ray_initted:
        ray.shutdown()
    return output