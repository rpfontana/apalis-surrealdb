UPDATE $worker SET last_seen = time::now() WHERE queue = $queue RETURN AFTER;
