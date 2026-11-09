// Package sheet builds the recovery sheet (ID-7, S3-8): the page for
// paper that rebuilds a node with lendisk (`lendisk recover`, ID-5) or
// restores its backups without it (P-5, LEND-6), and the record that
// tells when a printed sheet is out of date (DESIGN §3.1 ID-7).
//
// Rules a caller must keep:
//
//   - The phrase is never stored (ID-1, ID-3): a Sheet carries it only
//     when the user typed it for this print, and Record never writes it,
//     nor an overflow target's keys.
//   - An overflow target's keys are on a Sheet only when the user asked
//     for them (`--with-secrets`, S3-8).
//   - The text is plain ASCII within Width columns, for paper.
package sheet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
)

// Width is the page's width in columns.
const Width = 80

// nodeKey is the node table's key for the last printed sheet.
const nodeKey = "recovery_sheet"

// CombineCp and CombineRsync are the sheet's commands that combine the
// replica directories replica-1, replica-2, … into one repository,
// repo, leaving out the housekeeping directories of lendisk (its
// tombstones) and versitygw (gateway.ReservedDirs). A plain union is
// right: one key has the same bytes wherever it is held (EP-11).
const (
	CombineCp = `mkdir -p repo
for r in replica-*/; do cp -R "$r". repo/; done
rm -rf repo/.lendisk repo/.sgwtmp repo/.vgwlocks`
	CombineRsync = `mkdir -p repo
for r in replica-*/; do
  rsync -a --exclude=/.lendisk/ --exclude=/.sgwtmp/ \
    --exclude=/.vgwlocks/ "$r" repo/
done`
)

// Sheet is what the page says; its JSON is `recovery-sheet --json`.
type Sheet struct {
	Printed time.Time `json:"printed_at"`
	Node    string    `json:"node"`
	// Phrase is set only when the user typed it for this print.
	Phrase   string     `json:"phrase,omitempty"`
	Friends  []Friend   `json:"friends"`
	Overflow []Overflow `json:"overflow"`
	// Places is N: friends plus overflow targets, each holding a replica
	// of every bucket.
	Places  int      `json:"places"`
	Buckets []Bucket `json:"buckets"`
}

// Friend is a lender: what `lendisk recover` dials.
type Friend struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// Overflow is an overflow target (S3-1, S3-8); the keys only with
// --with-secrets.
type Overflow struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
}

// Bucket is one of the node's buckets and where it is stored.
type Bucket struct {
	Name    string `json:"name"`
	Profile string `json:"profile"`
	Copies  int    `json:"copies"`
	Sealed  string `json:"sealed"`
	// At is the name it has at each place (SEC-16): lent-<id16>-<sealed>
	// at every friend, an s3:// path at an overflow target.
	At []Place `json:"at"`
	// Needed is how many places' replicas a restore without lendisk
	// combines: N - c + 1, one when N <= c, none with no place.
	Needed int `json:"needed"`
}

// Place is a bucket's name at one place: a friend's node ID, or
// "overflow <name>".
type Place struct {
	Place string `json:"place"`
	Name  string `json:"name"`
}

// New makes the sheet of node from its manifest's content; an overflow
// target's keys are kept only when secrets is set.
func New(node identity.NodeID, c manifest.Content, secrets bool) Sheet {
	s := Sheet{Node: node.String(), Friends: []Friend{}, Overflow: []Overflow{}, Buckets: []Bucket{}}
	for _, f := range c.Friends {
		s.Friends = append(s.Friends, Friend{ID: f.ID, Address: f.Address})
	}
	for _, o := range c.Overflow {
		so := Overflow{Name: o.Name, Endpoint: o.Endpoint, Bucket: o.Bucket, Prefix: o.Prefix}
		if secrets {
			so.AccessKey, so.SecretKey = o.AccessKey, o.SecretKey
		}
		s.Overflow = append(s.Overflow, so)
	}
	s.Places = len(s.Friends) + len(s.Overflow)
	for _, b := range c.Buckets {
		sb := Bucket{Name: b.Name, Profile: b.Profile, Copies: b.Copies, Sealed: b.Sealed, At: []Place{},
			Needed: needed(s.Places, b.Copies)}
		for _, f := range s.Friends {
			sb.At = append(sb.At, Place{Place: f.ID, Name: gateway.LentName(node, b.Sealed)})
		}
		for _, o := range s.Overflow {
			sb.At = append(sb.At, Place{Place: "overflow " + o.Name, Name: overflowPath(o, b.Sealed)})
		}
		s.Buckets = append(s.Buckets, sb)
	}
	return s
}

// overflowPath is where a bucket's keys are at an overflow target: the
// sub-prefix <prefix>/<sealed>/ of its bucket (DESIGN §3.11 S3-1).
func overflowPath(o Overflow, sealed string) string {
	p := sealed
	if pre := strings.Trim(o.Prefix, "/"); pre != "" {
		p = pre + "/" + sealed
	}
	return "s3://" + o.Bucket + "/" + p + "/"
}

// needed is how many replicas a restore without lendisk combines with
// copies c over n places (P-5, REPL-1 AC): any n - c + 1 hold every
// object, any one when n <= c.
func needed(places, copies int) int {
	if places == 0 {
		return 0
	}
	return max(1, places-max(copies, 1)+1)
}

// Build reads the node's sheet from its state, as its manifest says it
// (ID-4).
func Build(ctx context.Context, db *state.DB, self *identity.Identity, secrets bool) (Sheet, error) {
	c, err := manifest.Build(ctx, db, self)
	if err != nil {
		return Sheet{}, err
	}
	return New(self.NodeID(), c, secrets), nil
}

// content is what the sheet says that can go out of date: no time, no
// phrase, no keys.
func (s Sheet) content() Sheet {
	c := s
	c.Printed, c.Phrase = time.Time{}, ""
	c.Overflow = slices.Clone(s.Overflow)
	for i := range c.Overflow {
		c.Overflow[i].AccessKey, c.Overflow[i].SecretKey = "", ""
	}
	return c
}

// printed is the node table's record of the last printed sheet.
type printed struct {
	At    time.Time `json:"printed_at"`
	Sheet Sheet     `json:"sheet"`
}

// Record notes that s was printed at at, so Stale can tell when the
// node's state no longer matches it. The phrase and keys are not
// recorded.
func Record(ctx context.Context, db *state.DB, s Sheet, at time.Time) error {
	b, err := json.Marshal(printed{At: at.UTC(), Sheet: s.content()})
	if err != nil {
		return err
	}
	return db.SetNode(ctx, nodeKey, string(b))
}

// Stale compares the last printed sheet with the node's state now: it
// returns when it was printed and what changed since, nil if nothing.
// A node that never printed one has a zero time and nothing stale.
func Stale(ctx context.Context, db *state.DB, self *identity.Identity) (time.Time, []string, error) {
	v, ok, err := db.GetNode(ctx, nodeKey)
	if err != nil || !ok {
		return time.Time{}, nil, err
	}
	live, err := Build(ctx, db, self, false)
	if err != nil {
		return time.Time{}, nil, err
	}
	var p printed
	if err := json.Unmarshal([]byte(v), &p); err != nil {
		return time.Time{}, []string{"the record of the printed sheet cannot be read"}, nil
	}
	return p.At, changes(p.Sheet, live.content()), nil
}

// changes says what differs from old to now, people first.
func changes(old, now Sheet) []string {
	var out []string
	if old.Node != now.Node {
		out = append(out, "the node ID changed")
	}
	oldF := map[string]Friend{}
	for _, f := range old.Friends {
		oldF[f.ID] = f
	}
	nowF := map[string]bool{}
	for _, f := range now.Friends {
		nowF[f.ID] = true
		switch o, ok := oldF[f.ID]; {
		case !ok:
			out = append(out, "friend "+f.ID+" added")
		case o.Address != f.Address:
			out = append(out, "friend "+f.ID+"'s address changed")
		}
	}
	for _, f := range old.Friends {
		if !nowF[f.ID] {
			out = append(out, "friend "+f.ID+" removed")
		}
	}
	oldO := map[string]Overflow{}
	for _, o := range old.Overflow {
		oldO[o.Name] = o
	}
	nowO := map[string]bool{}
	for _, o := range now.Overflow {
		nowO[o.Name] = true
		switch p, ok := oldO[o.Name]; {
		case !ok:
			out = append(out, "overflow target "+o.Name+" added")
		case p != o:
			out = append(out, "overflow target "+o.Name+" changed")
		}
	}
	for _, o := range old.Overflow {
		if !nowO[o.Name] {
			out = append(out, "overflow target "+o.Name+" removed")
		}
	}
	oldB := map[string]Bucket{}
	for _, b := range old.Buckets {
		oldB[b.Name] = b
	}
	nowB := map[string]bool{}
	for _, b := range now.Buckets {
		nowB[b.Name] = true
		switch p, ok := oldB[b.Name]; {
		case !ok:
			out = append(out, "bucket "+b.Name+" added")
		case p.Copies != b.Copies || p.Profile != b.Profile || p.Sealed != b.Sealed:
			out = append(out, "bucket "+b.Name+" changed")
		}
	}
	for _, b := range old.Buckets {
		if !nowB[b.Name] {
			out = append(out, "bucket "+b.Name+" removed")
		}
	}
	if out == nil {
		a, _ := json.Marshal(old)
		b, _ := json.Marshal(now)
		if string(a) != string(b) {
			out = append(out, "the sheet's content changed")
		}
	}
	return out
}

// WriteText writes the page: plain ASCII within Width columns.
func (s Sheet) WriteText(w io.Writer) error {
	p := &page{}
	title := "LENDISK RECOVERY SHEET"
	if !s.Printed.IsZero() {
		date := "printed " + s.Printed.UTC().Format("2006-01-02 15:04 UTC")
		title += strings.Repeat(" ", Width-len(title)-len(date)) + date
	}
	p.line(title)
	p.line(strings.Repeat("=", Width))
	p.para("", "Keep this page offline and safe. With it you can rebuild this node on a new machine, "+
		"or restore your backups with no lendisk at all.")

	p.heading("1. Recovery phrase (24 words)")
	words := strings.Fields(s.Phrase)
	if len(words) == 0 {
		p.para("   ", "The phrase is stored nowhere, not even by lendisk: write it here by hand.")
		words = make([]string, 24)
		for i := range words {
			words[i] = strings.Repeat("_", 14)
		}
	} else {
		p.para("   ", "Anyone holding this page holds the phrase and can act as this node: keep it as you would the phrase itself.")
	}
	for i := 0; i < len(words); i += 4 {
		var b strings.Builder
		b.WriteString("  ")
		for j := i; j < min(i+4, len(words)); j++ {
			fmt.Fprintf(&b, " %2d. %-14s", j+1, words[j])
		}
		p.line(strings.TrimRight(b.String(), " "))
	}

	p.heading("2. This node")
	p.line("   Node ID: " + s.Node)

	p.heading(fmt.Sprintf("3. Friends: %d", len(s.Friends)))
	if len(s.Friends) == 0 {
		p.para("   ", "None: no friend holds a replica yet.")
	} else {
		p.para("   ", "Each holds a replica of every bucket. `lendisk recover` needs any one address. "+
			"An address is one string: type it without the line breaks.")
		for i, f := range s.Friends {
			p.line("")
			p.line(fmt.Sprintf("   Friend %d: %s", i+1, f.ID))
			if f.Address == "" {
				p.line("   Address: not known")
				continue
			}
			p.line("   Address:")
			p.chunks("     ", f.Address)
		}
	}

	if len(s.Overflow) == 0 {
		p.heading("4. Overflow targets: none")
	} else {
		p.heading(fmt.Sprintf("4. Overflow targets: %d", len(s.Overflow)))
		for _, o := range s.Overflow {
			p.line("")
			p.line("   " + o.Name + ":")
			p.field("     endpoint:   ", o.Endpoint)
			p.field("     bucket:     ", o.Bucket)
			if o.Prefix != "" {
				p.field("     prefix:     ", o.Prefix)
			}
			if o.AccessKey != "" || o.SecretKey != "" {
				p.field("     access key: ", o.AccessKey)
				p.field("     secret key: ", o.SecretKey)
			} else {
				p.para("     ", "Its access key is needed to read it, and is not on this page "+
					"(`lendisk recovery-sheet --with-secrets` prints it); `lendisk recover` restores it from the friends.")
			}
		}
	}

	p.heading("5. Restore with lendisk")
	p.line("   On a new machine with lendisk installed:")
	p.line("     lendisk recover <a friend's address>     (asks for the phrase)")
	p.line("     lendisk serve")
	p.para("   ", "Then `lendisk bucket show <bucket>` prints each bucket's credentials for "+
		"the backup tool, which restores as usual:")
	p.line("     restic restore latest --target <directory>")
	p.line("     kopia snapshot restore <snapshot ID> <directory>")

	p.heading(fmt.Sprintf("6. Buckets: %d", len(s.Buckets)))
	if len(s.Buckets) == 0 {
		p.para("   ", "None yet.")
	}
	for _, b := range s.Buckets {
		p.line("")
		p.line(fmt.Sprintf("   %s (%s, copies %d)", b.Name, b.Profile, b.Copies))
		p.field("     sealed name: ", b.Sealed)
		if len(s.Friends) > 0 {
			p.field("     at every friend: ", b.At[0].Name)
		}
		for _, at := range b.At[len(s.Friends):] {
			p.field("     at "+strings.TrimPrefix(at.Place, "overflow ")+": ", at.Name)
		}
		switch {
		case b.Needed == 0:
			p.para("     ", "Without lendisk: no place holds it yet; this node's store is its only copy.")
		case b.Needed == 1 && s.Places == 1:
			p.para("     ", "Without lendisk: the replica of the one place.")
		case b.Needed == 1:
			p.para("     ", fmt.Sprintf("Without lendisk: the replica of any 1 of the %d places.", s.Places))
		default:
			p.para("     ", fmt.Sprintf("Without lendisk: the replicas of any %d of the %d places, combined.", b.Needed, s.Places))
		}
	}

	p.heading("7. Restore without lendisk")
	p.para("   ", "Every place holds a replica of each bucket: all of its metadata and the data "+
		"placed there. The replicas of any N - c + 1 of the N places hold every object "+
		"(c is the bucket's copies; each bucket's count is above).")
	p.para("   ", "To fetch a replica from a friend, with no lendisk on your side, the friend runs")
	p.line("     lendisk export --lent <stored name> <directory>")
	p.para("   ", "(adding --kopia-layout for a Kopia bucket) and hands you the directory. "+
		"For a restic bucket, a copy of <their store>/<stored name>/ made with their lendisk "+
		"stopped does as well. From an overflow target, copy the keys under its s3:// path "+
		"above with any S3 client and its access key.")
	p.para("   ", "Put each replica in a directory of its own, replica-1, replica-2 and so on, "+
		"side by side, and combine them, leaving out the housekeeping directories:")
	p.block("     ", CombineCp)
	p.line("   or, with rsync:")
	p.block("     ", CombineRsync)
	p.line("   Then:")
	p.line("     restic -r repo restore latest --target <directory>")
	p.line("   or:")
	p.line("     kopia repository connect filesystem --path repo")

	p.heading("8. The backup tool's password")
	p.para("   ", "Your restic or Kopia repository password is a separate secret. It is not on "+
		"this sheet and lendisk never had it: without it no backup can be read. Keep it "+
		"as safe as this page, somewhere else.")

	p.line("")
	p.line(strings.Repeat("-", Width))
	p.para("", "Print this sheet again when lendisk alerts that it is out of date: a friend's "+
		"address changed, or a friend, overflow target or bucket was added or removed.")
	_, err := io.WriteString(w, p.String())
	return err
}

// page builds the text line by line.
type page struct{ strings.Builder }

func (p *page) line(s string) {
	p.WriteString(s)
	p.WriteByte('\n')
}

func (p *page) heading(s string) {
	p.line("")
	p.line(s)
}

// para word-wraps text to Width, each line after indent.
func (p *page) para(indent, text string) {
	cur := indent
	for _, w := range strings.Fields(text) {
		if cur != indent && len(cur)+1+len(w) > Width {
			p.line(cur)
			cur = indent
		}
		if cur != indent {
			cur += " "
		}
		cur += w
	}
	if cur != indent {
		p.line(cur)
	}
}

// field writes label then value, the value cut into lines aligned
// under its start when it is longer than the line.
func (p *page) field(label, value string) {
	if len(label)+len(value) <= Width {
		p.line(label + value)
		return
	}
	first := Width - len(label)
	p.line(label + value[:first])
	p.chunks(strings.Repeat(" ", len(label)), value[first:])
}

// chunks writes a string with no spaces (an address) cut into lines
// after indent.
func (p *page) chunks(indent, s string) {
	n := Width - len(indent)
	for len(s) > n {
		p.line(indent + s[:n])
		s = s[n:]
	}
	if s != "" {
		p.line(indent + s)
	}
}

// block writes a multi-line command, each line after indent.
func (p *page) block(indent, text string) {
	for _, l := range strings.Split(text, "\n") {
		p.line(indent + l)
	}
}
