UPSERT $worker CONTENT {
    name: $name,
    queue: $queue,
    storage_name: $storage,
    layers: $layers,
    instance: $instance,
    started_at: time::now(),
    last_seen: time::now()
};
