package state

import "strings"

// This file is schema 57's SQL for overflow_live and overflow_hold
// (#119), shared by the migration's triggers (migration57) and
// RepairQueueTally's correction, so the two cannot differ. Once
// released, never change it: a change belongs in a new migration with
// its own copy.

// overflowPlaceRange is the condition that place is an overflow
// target's ('overflow:' and anything): a range, so an index on place
// serves it.
func overflowPlaceRange(place string) string {
	return "(" + place + " >= 'overflow:' AND " + place + " < 'overflow;')"
}

// overflowLiveSelect selects the copies that count at a target
// (overflow_live's rows) among placement p and queue q, where cond
// holds: a put's row queued or acked at a target, of the data class,
// its key's latest done write (liveQ).
func overflowLiveSelect(cond string) string {
	return `SELECT p.place AS place, p.seq AS seq, q.bucket AS bucket, q.key AS key, q.size AS size
			FROM queue q JOIN placement p ON p.seq = q.seq
			WHERE ` + cond + ` AND ` + overflowPlaceRange("p.place") + `
			AND p.state IN ('queued', 'acked') AND p.op = 'put' AND q.class = 'data'
			AND NOT EXISTS (SELECT 1 FROM queue l
				WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`
}

// anyOverflowCopy holds when any placement row or stored copy is at an
// overflow target. The queue triggers run only then: with none, no
// queue write can change overflow_live (every row of it has a
// placement row at a target), so a node without overflow pays two
// index probes per queue write, not the key's recompute (#119 review).
const anyOverflowCopy = `(EXISTS (SELECT 1 FROM placement WHERE place >= 'overflow:' AND place < 'overflow;')
		OR EXISTS (SELECT 1 FROM overflow_live))`

// overflowLiveRows is every copy that counts at a target.
var overflowLiveRows = overflowLiveSelect("1")

// overflowLiveDrop takes the overflow_live rows where w holds ($T names
// the table) away from overflow_hold, and deletes them.
func overflowLiveDrop(w string) []string {
	h := strings.ReplaceAll(w, "$T", "h")
	return []string{
		`UPDATE overflow_hold SET
				objects = objects - (SELECT count(*) FROM overflow_live h WHERE h.place = overflow_hold.place AND ` + h + `),
				bytes = bytes - (SELECT coalesce(sum(h.size), 0) FROM overflow_live h WHERE h.place = overflow_hold.place AND ` + h + `)
			WHERE place IN (SELECT h.place FROM overflow_live h WHERE ` + h + `)`,
		`DELETE FROM overflow_live WHERE ` + strings.ReplaceAll(w, "$T", "overflow_live"),
	}
}

// overflowLiveAdd inserts the copies rows selects that are not already
// stored, and adds the overflow_live rows where w holds (the ones just
// inserted, once overflowLiveDrop(w) ran) to overflow_hold. Neither
// insert meets a conflict.
func overflowLiveAdd(rows, w string) []string {
	h := strings.ReplaceAll(w, "$T", "h")
	return []string{
		`INSERT INTO overflow_live (place, seq, bucket, key, size)
			SELECT r.place, r.seq, r.bucket, r.key, r.size FROM (` + rows + `) r
			WHERE NOT EXISTS (SELECT 1 FROM overflow_live x WHERE x.place = r.place AND x.seq = r.seq)`,
		`INSERT INTO overflow_hold (place) SELECT DISTINCT h.place FROM overflow_live h WHERE ` + h + `
			AND NOT EXISTS (SELECT 1 FROM overflow_hold o WHERE o.place = h.place)`,
		`UPDATE overflow_hold SET
				objects = objects + (SELECT count(*) FROM overflow_live h WHERE h.place = overflow_hold.place AND ` + h + `),
				bytes = bytes + (SELECT coalesce(sum(h.size), 0) FROM overflow_live h WHERE h.place = overflow_hold.place AND ` + h + `)
			WHERE place IN (SELECT h.place FROM overflow_live h WHERE ` + h + `)`,
	}
}

// andCond is " AND cond", or "" for no condition.
func andCond(cond string) string {
	if cond == "" {
		return ""
	}
	return " AND " + cond
}

// overflowKeyDrop and overflowKeyAdd recompute every copy of key in
// bucket (SQL expressions), where cond holds: drop them all, then add
// those that count.
func overflowKeyDrop(bucket, key, cond string) []string {
	return overflowLiveDrop("$T.bucket = " + bucket + " AND $T.key = " + key + andCond(cond))
}

func overflowKeyAdd(bucket, key, cond string) []string {
	return overflowLiveAdd(overflowLiveSelect("q.bucket = "+bucket+" AND q.key = "+key+andCond(cond)),
		"$T.bucket = "+bucket+" AND $T.key = "+key+andCond(cond))
}

// overflowCopy recomputes the copy at place of queue row seq, where
// cond holds.
func overflowCopy(place, seq, cond string) []string {
	w := "$T.place = " + place + " AND $T.seq = " + seq + andCond(cond)
	return append(overflowLiveDrop(w),
		overflowLiveAdd(overflowLiveSelect("p.place = "+place+" AND p.seq = "+seq+andCond(cond)), w)...)
}
