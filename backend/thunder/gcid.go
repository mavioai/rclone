package thunder

import (
	"crypto/sha1"
	"encoding"
	gohash "hash"
)

// GCID is the content hash Xunlei uses to identify files.
//
// The content is split into blocks and GCID is the SHA-1 of the
// concatenated SHA-1 digests of the blocks. The block size depends on
// the total size: 256 KiB, doubled while the file would have more
// than 512 blocks, up to a maximum of 2 MiB.
//
// As the total size is only known at the end, gcidHasher hashes the
// stream with every candidate block size at once and drops a
// candidate as soon as the data written rules it out, so large files
// are only hashed once per byte (plus the per-block digest).
const (
	gcidMinBlockSize = 0x40000  // 256 KiB
	gcidMaxBlockSize = 0x200000 // 2 MiB
	gcidMaxBlocks    = 0x200    // 512
)

// gcidChain hashes the stream with one candidate block size
type gcidChain struct {
	blockSize int64       // size of the blocks
	limit     int64       // largest total size using this block size, or -1 for no limit
	block     gohash.Hash // hash of the current block
	inBlock   int64       // bytes written to the current block
	total     gohash.Hash // hash of the block digests
	dead      bool        // set if the total size rules this block size out
}

// gcidHasher computes the GCID of a stream as a hash.Hash
type gcidHasher struct {
	written int64
	chains  []*gcidChain
}

// newGcidHasher returns a hash.Hash computing the GCID
func newGcidHasher() gohash.Hash {
	h := &gcidHasher{}
	for bs := int64(gcidMinBlockSize); bs <= gcidMaxBlockSize; bs <<= 1 {
		limit := bs * gcidMaxBlocks
		if bs == gcidMaxBlockSize {
			limit = -1
		}
		h.chains = append(h.chains, &gcidChain{
			blockSize: bs,
			limit:     limit,
			block:     sha1.New(),
			total:     sha1.New(),
		})
	}
	return h
}

// Write adds more data to the running hash. It never returns an error.
func (h *gcidHasher) Write(p []byte) (int, error) {
	h.written += int64(len(p))
	for _, c := range h.chains {
		if c.dead {
			continue
		}
		if c.limit >= 0 && h.written > c.limit {
			c.dead = true
			continue
		}
		c.write(p)
	}
	return len(p), nil
}

// write feeds p into the blocks of the chain
func (c *gcidChain) write(p []byte) {
	for len(p) > 0 {
		n := min(int64(len(p)), c.blockSize-c.inBlock)
		_, _ = c.block.Write(p[:n])
		c.inBlock += n
		p = p[n:]
		if c.inBlock == c.blockSize {
			_, _ = c.total.Write(c.block.Sum(nil))
			c.block.Reset()
			c.inBlock = 0
		}
	}
}

// Sum appends the GCID to b without changing the state of the hash
func (h *gcidHasher) Sum(b []byte) []byte {
	for _, c := range h.chains {
		if !c.dead {
			return c.sum(b)
		}
	}
	// not reached: the last chain never dies
	return nil
}

// sum appends the digest of the chain to b
func (c *gcidChain) sum(b []byte) []byte {
	if c.inBlock == 0 {
		return c.total.Sum(b)
	}
	// Clone the total hash so the partial block doesn't change the state
	state, err := c.total.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}
	total := sha1.New()
	if err := total.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		panic(err)
	}
	_, _ = total.Write(c.block.Sum(nil))
	return total.Sum(b)
}

// Reset resets the hash to its initial state
func (h *gcidHasher) Reset() {
	h.written = 0
	for _, c := range h.chains {
		c.block.Reset()
		c.total.Reset()
		c.inBlock = 0
		c.dead = false
	}
}

// Size returns the number of bytes Sum will return
func (h *gcidHasher) Size() int { return sha1.Size }

// BlockSize returns the hash's underlying block size
func (h *gcidHasher) BlockSize() int { return sha1.BlockSize }
