package loggerhead

import (
	"cloud.google.com/go/spanner"
	"golang.org/x/net/context"
	"testing"
	"time"
)

const (
	actuallyRun = false
	dbName      = "projects/loggerhead-159916/instances/loggerhead/databases/loggerhead"
)

func ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	return ctx
}

func getDB() (*spanner.Client, error) {
	return spanner.NewClient(ctx(), dbName)
}

func clearDB(db *spanner.Client) error {
	_, err := db.ReadWriteTransaction(ctx(), func(txn *spanner.ReadWriteTransaction) error {
		prettyMuchEverything := spanner.KeyRange{
			Start: spanner.Key{0},
			End:   spanner.Key{10000000},
			Kind:  spanner.ClosedClosed,
		}

		err := txn.BufferWrite([]*spanner.Mutation{
			spanner.DeleteKeyRange("frontier", prettyMuchEverything),
			spanner.DeleteKeyRange("certificates", prettyMuchEverything),
		})
		return err
	})

	return err
}

func TestDB(t *testing.T) {
	// XXX: This is guarded because it mutates the database.  Only safe to use if
	// the DB is in pristine state and/or if you don't care about clearing it out.
	if !actuallyRun {
		return
	}

	client, err := spanner.NewClient(ctx(), dbName)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ReadWriteTransaction(ctx(), func(txn *spanner.ReadWriteTransaction) error {
		// Read the frontier
		f, _, err := readFrontier(txn)

		// Add a certificate
		cert := []byte{0, 1, 2, 3}
		f.Add(cert)

		// Update the frontier
		frontierCols := []string{"index", "subtree_size", "subhead"}
		mutations := make([]*spanner.Mutation, f.Len())
		for i, entry := range f.entries {
			vals := []interface{}{i, int64(entry.SubtreeSize), entry.Value}
			mutations[i] = spanner.InsertOrUpdate("frontier", frontierCols, vals)
		}
		err = txn.BufferWrite(mutations)
		if err != nil {
			return err
		}

		// Write the certificate
		timestamp := time.Now().Unix()
		certCols := []string{"timestamp", "tree_size", "tree_head", "cert"}
		certVals := []interface{}{timestamp, int64(f.Size()), f.Head(), cert}
		mutations = []*spanner.Mutation{spanner.Insert("certificates", certCols, certVals)}
		err = txn.BufferWrite(mutations)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ReadWriteTransaction(ctx(), func(txn *spanner.ReadWriteTransaction) error {
		everything := spanner.KeyRange{
			Start: spanner.Key{int64(-0x8000000000000000)},
			End:   spanner.Key{int64(0x7fffffffffffffff)},
			Kind:  spanner.ClosedClosed,
		}

		err := txn.BufferWrite([]*spanner.Mutation{
			spanner.DeleteKeyRange("frontier", everything),
			spanner.DeleteKeyRange("certificates", everything),
		})
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
}
