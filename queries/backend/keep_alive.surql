UPDATE $worker SET last_seen = time::now() WHERE queue = $queue AND instance = $instance RETURN AFTER;
