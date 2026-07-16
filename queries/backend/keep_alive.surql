UPDATE $worker SET last_seen = time::now() RETURN AFTER;
