BEGIN;
FOR $task IN $tasks {
    IF $task.idempotency_key IS NONE
        OR count((SELECT id FROM job WHERE queue = $task.queue AND idempotency_key = $task.idempotency_key)) = 0 {
        CREATE $task.id CONTENT {
            args: $task.args,
            queue: $task.queue,
            status: 'Pending',
            attempts: 0,
            max_attempts: $task.max_attempts,
            run_at: $task.run_at,
            priority: $task.priority,
            metadata: $task.metadata,
            idempotency_key: $task.idempotency_key
        };
    };
};
COMMIT;
