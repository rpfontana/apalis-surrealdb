SELECT record::id(id) AS id, status, last_result AS result FROM $ids
    WHERE status = 'Done' OR (status = 'Failed' AND attempts >= max_attempts) OR status = 'Killed';
