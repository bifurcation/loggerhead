package loggerhead

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/net/context"
)

const (
	doClearDB = true
	db        = "projects/loggerhead-159916/instances/loggerhead/databases/loggerhead"
)

// Before running, set GOOGLE_APPLICATION_CREDENTIALS
// https://developers.google.com/identity/protocols/application-default-credentials
func TestSpanner(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)

	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ReadWriteTransaction(ctx, func(txn *spanner.ReadWriteTransaction) error {
		// Read the frontier
		f := frontier{}
		keySet := spanner.KeySet{All: true}
		iter := txn.Read(ctx, "frontier", keySet, []string{"index", "subtree_size", "subhead"})
		err := iter.Do(func(row *spanner.Row) error {
			var index int64
			var subtreeSize int64
			var subhead []byte
			err := row.Columns(&index, &subtreeSize, &subhead)
			if err != nil {
				return err
			}

			f.entries = append(f.entries, frontierEntry{uint64(subtreeSize), subhead})
			return nil
		})
		if err != nil {
			return err
		}

		f.Sort()
		fmt.Printf("read frontier: %+v\n", f)

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
		fmt.Printf("buffered frontier write\n")

		// Write the certificate
		timestamp := time.Now().Unix()
		certCols := []string{"timestamp", "tree_size", "tree_head", "cert"}
		certVals := []interface{}{timestamp, int64(f.Size()), f.Head(), cert}
		mutations = []*spanner.Mutation{spanner.Insert("certificates", certCols, certVals)}
		err = txn.BufferWrite(mutations)
		if err != nil {
			return err
		}
		fmt.Printf("buffered cert write\n")

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ReadWriteTransaction(ctx, func(txn *spanner.ReadWriteTransaction) error {
		prettyMuchEverything := spanner.KeyRange{
			Start: spanner.Key{0},
			End:   spanner.Key{10000000},
			Kind:  spanner.ClosedClosed,
		}

		err := txn.BufferWrite([]*spanner.Mutation{
			spanner.DeleteKeyRange("frontier", prettyMuchEverything),
			spanner.DeleteKeyRange("certificates", prettyMuchEverything),
		})
		fmt.Printf("buffered DB clear-out\n")
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
}
