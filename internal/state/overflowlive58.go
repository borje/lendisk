package state

// This file is schema 58's SQL (#120): the queue triggers of
// overflow_live and overflow_hold recompute a key's copies from its
// latest done write on, not from every version of it. Shared by the
// migration's triggers (migration58) and RepairQueueTally's correction,
// so the two cannot differ. It builds on overflowlive.go's statements,
// which never change. Once released, never change it: a change belongs
// in a new migration with its own copy.

// latestDone is the seq of the latest done write of key in bucket (SQL
// expressions), or the least integer when it has none. It walks the
// key's rows by queue_key from its newest down to that write: one row,
// as a rule, however many versions the key has.
func latestDone(bucket, key string) string {
	return `coalesce((SELECT l.seq FROM queue l WHERE l.bucket = ` + bucket + ` AND l.key = ` + key + `
				AND l.state = 'done' ORDER BY l.seq DESC LIMIT 1), -9223372036854775808)`
}

// overflowKeyAdd58 is overflowKeyAdd reading only the key's rows from
// its latest done write on: an earlier one has a later done write, so
// it is not its key's latest (liveQ) and no copy of it counts. The rows
// are found by queue_key's range on (bucket, key, seq), so a write to a
// key costs what its live copies do, not what its versions do.
func overflowKeyAdd58(bucket, key, cond string) []string {
	return overflowLiveAdd(overflowLiveSelect("q.bucket = "+bucket+" AND q.key = "+key+
		" AND q.seq >= "+latestDone(bucket, key)+andCond(cond)),
		"$T.bucket = "+bucket+" AND $T.key = "+key+andCond(cond))
}
