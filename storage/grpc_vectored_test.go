// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// trackingBufferPool tracks active buffer allocations to verify zero-copy
// reference counting and ensure no buffers leak.
type trackingBufferPool struct {
	got     atomic.Int64
	put     atomic.Int64
	active  atomic.Int64
	buffers sync.Map
}

func (p *trackingBufferPool) Get(length int) *[]byte {
	capacity := length
	if capacity < 32*1024 {
		capacity = 32 * 1024
	}
	b := make([]byte, length, capacity)
	p.got.Add(1)
	p.active.Add(1)
	p.buffers.Store(&b, true)
	return &b
}

func (p *trackingBufferPool) Put(b *[]byte) {
	if _, loaded := p.buffers.LoadAndDelete(b); !loaded {
		panic("trackingBufferPool: double Put or unknown buffer")
	}
	p.put.Add(1)
	p.active.Add(-1)
}

func (p *trackingBufferPool) makeBufferSlice(chunks ...[]byte) mem.BufferSlice {
	var slice mem.BufferSlice
	for _, c := range chunks {
		bufPtr := p.Get(len(c))
		copy(*bufPtr, c)
		slice = append(slice, mem.NewBuffer(bufPtr, p))
	}
	return slice
}

func splitMarshal(t *testing.T, pool *trackingBufferPool, msg proto.Message, splitSize int) mem.BufferSlice {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	if splitSize <= 0 || splitSize >= len(raw) {
		return pool.makeBufferSlice(raw)
	}
	var chunks [][]byte
	for i := 0; i < len(raw); i += splitSize {
		end := i + splitSize
		if end > len(raw) {
			end = len(raw)
		}
		chunks = append(chunks, raw[i:end])
	}
	return pool.makeBufferSlice(chunks...)
}

type fakeReadObjectStream struct {
	grpc.ClientStream
	msgs []mem.BufferSlice
	idx  int
}

func (f *fakeReadObjectStream) RecvMsg(m any) error {
	if f.idx >= len(f.msgs) {
		return io.EOF
	}
	dst, ok := m.(*mem.BufferSlice)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	*dst = f.msgs[f.idx]
	f.idx++
	return nil
}

func (f *fakeReadObjectStream) Recv() (*storagepb.ReadObjectResponse, error) {
	return nil, io.EOF
}

type fakeBidiReadStream struct {
	grpc.ClientStream
	msgs []mem.BufferSlice
	idx  int
}

func (f *fakeBidiReadStream) RecvMsg(m any) error {
	if f.idx >= len(f.msgs) {
		return io.EOF
	}
	dst, ok := m.(*mem.BufferSlice)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	*dst = f.msgs[f.idx]
	f.idx++
	return nil
}

func (f *fakeBidiReadStream) Send(*storagepb.BidiReadObjectRequest) error {
	return nil
}

func (f *fakeBidiReadStream) Recv() (*storagepb.BidiReadObjectResponse, error) {
	return nil, io.EOF
}

func (f *fakeBidiReadStream) CloseSend() error {
	return nil
}

func makeReadObjectMsg(t *testing.T, pool *trackingBufferPool, content []byte, crc *uint32, splitSize int) mem.BufferSlice {
	t.Helper()
	msg := &storagepb.ReadObjectResponse{
		ChecksummedData: &storagepb.ChecksummedData{
			Content: content,
			Crc32C:  crc,
		},
	}
	return splitMarshal(t, pool, msg, splitSize)
}

func makeBidiReadMsg(t *testing.T, pool *trackingBufferPool, readID int64, offset int64, content []byte, crc *uint32, rangeEnd bool, splitSize int) mem.BufferSlice {
	t.Helper()
	msg := &storagepb.BidiReadObjectResponse{
		ObjectDataRanges: []*storagepb.ObjectRangeData{
			{
				ChecksummedData: &storagepb.ChecksummedData{
					Content: content,
					Crc32C:  crc,
				},
				ReadRange: &storagepb.ReadRange{
					ReadId:     readID,
					ReadOffset: offset,
				},
				RangeEnd: rangeEnd,
			},
		},
	}
	return splitMarshal(t, pool, msg, splitSize)
}

func newTestReadObjectReader(t *testing.T, msgs []mem.BufferSlice, totalSize int64, checkCRC bool, wantCRC uint32) (*Reader, *gRPCReadObjectReader) {
	t.Helper()
	gr := &gRPCReadObjectReader{
		size:      totalSize,
		stream:    &fakeReadObjectStream{msgs: msgs},
		settings:  &settings{retry: &retryConfig{}},
		checkCRC:  checkCRC,
		wantCRC:   wantCRC,
	}
	sr := &Reader{
		Attrs:    ReaderObjectAttrs{Size: totalSize},
		remain:   totalSize,
		checkCRC: checkCRC,
		reader:   gr,
	}
	return sr, gr
}

func newTestBidiReader(t *testing.T, msgs []mem.BufferSlice, totalSize int64, checkCRC bool, wantCRC uint32) (*Reader, *gRPCReader) {
	t.Helper()
	gr := &gRPCReader{
		size:      totalSize,
		finalized: true,
		stream:    &fakeBidiReadStream{msgs: msgs},
		settings:  &settings{retry: &retryConfig{}},
		checkCRC:  checkCRC,
		wantCRC:   wantCRC,
	}
	sr := &Reader{
		Attrs:    ReaderObjectAttrs{Size: totalSize},
		remain:   totalSize,
		checkCRC: checkCRC,
		reader:   gr,
	}
	return sr, gr
}

func flattenSlices(slices [][]byte) []byte {
	var out []byte
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

func assertCappedSlices(t *testing.T, slices [][]byte) {
	t.Helper()
	if len(slices) != cap(slices) {
		t.Errorf("outer slices len=%d != cap=%d", len(slices), cap(slices))
	}
	for i, s := range slices {
		if len(s) != cap(s) {
			t.Errorf("slices[%d] len=%d != cap=%d", i, len(s), cap(s))
		}
	}
}

func TestReadVectored_ZeroCopy_SingleAndMultiBuffer(t *testing.T) {
	payload1 := []byte("0123456789abcdef0123456789ABCDEF")
	payload2 := []byte("fedcba9876543210FEDCBA9876543210")
	wantAll := append(append([]byte(nil), payload1...), payload2...)

	for _, mode := range []string{"ReadObject", "BidiReadObject"} {
		for _, splitSize := range []int{0, 7} {
			t.Run(mode+"/split_"+string(rune('0'+splitSize)), func(t *testing.T) {
				pool := &trackingBufferPool{}
				var sr *Reader
				if mode == "ReadObject" {
					msgs := []mem.BufferSlice{
						makeReadObjectMsg(t, pool, payload1, nil, splitSize),
						makeReadObjectMsg(t, pool, payload2, nil, splitSize),
					}
					sr, _ = newTestReadObjectReader(t, msgs, int64(len(wantAll)), false, 0)
				} else {
					msgs := []mem.BufferSlice{
						makeBidiReadMsg(t, pool, defaultReadID, 0, payload1, nil, false, splitSize),
						makeBidiReadMsg(t, pool, defaultReadID, int64(len(payload1)), payload2, nil, true, splitSize),
					}
					sr, _ = newTestBidiReader(t, msgs, int64(len(wantAll)), false, 0)
				}

				// 1. Empty buf returns nil, 0, nil, nil without consuming buffers.
				slices, n, rel, err := sr.ReadVectored(nil)
				if slices != nil || n != 0 || rel != nil || err != nil {
					t.Fatalf("empty ReadVectored = (%v, %d, rel!=nil:%v, %v), want (nil, 0, false, nil)", slices, n, rel != nil, err)
				}

				// 2. Partial read across first message (10 bytes), then 30 bytes spanning msg1 & msg2, then rest.
				var got []byte
				var releases []func()
				for _, readSize := range []int{10, 30, 40} {
					buf := bytes.Repeat([]byte{'X'}, readSize)
					slices, n, rel, err := sr.ReadVectored(buf)
					if n == 0 {
						t.Fatalf("ReadVectored(%d) returned 0 bytes, err=%v", readSize, err)
					}
					if slices == nil || rel == nil {
						t.Fatalf("ReadVectored(%d) fell back to copy unexpectedly", readSize)
					}
					assertCappedSlices(t, slices)
					// Verify buf was not touched (zero-copy).
					for i, b := range buf {
						if b != 'X' {
							t.Fatalf("fallback buffer modified at index %d", i)
						}
					}
					chunk := flattenSlices(slices)
					if len(chunk) != n {
						t.Fatalf("flattened slices len=%d != n=%d", len(chunk), n)
					}
					got = append(got, chunk...)
					releases = append(releases, rel)
				}

				if !bytes.Equal(got, wantAll) {
					t.Errorf("got %q, want %q", got, wantAll)
				}
				if sr.Remain() != 0 {
					t.Errorf("Remain() = %d, want 0", sr.Remain())
				}
				if sr.bytesRead != int64(len(wantAll)) {
					t.Errorf("bytesRead = %d, want %d", sr.bytesRead, len(wantAll))
				}

				// Buffers holding payload slices must still be active before release() is called.
				if pool.active.Load() == 0 {
					t.Fatalf("expected active buffers > 0 before calling release callbacks")
				}

				// Release all held slices and close reader; all buffers must be returned to pool.
				for _, rel := range releases {
					rel()
				}
				if err := sr.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				if active := pool.active.Load(); active != 0 {
					t.Errorf("active buffers after release & Close = %d, want 0 (got=%d, put=%d)", active, pool.got.Load(), pool.put.Load())
				}
			})
		}
	}
}

func TestReadVectored_CloseMidMessageFreesUnreadBuffers(t *testing.T) {
	payload := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	for _, mode := range []string{"ReadObject", "BidiReadObject"} {
		t.Run(mode, func(t *testing.T) {
			pool := &trackingBufferPool{}
			var sr *Reader
			if mode == "ReadObject" {
				msgs := []mem.BufferSlice{makeReadObjectMsg(t, pool, payload, nil, 8)}
				sr, _ = newTestReadObjectReader(t, msgs, int64(len(payload)), false, 0)
			} else {
				msgs := []mem.BufferSlice{makeBidiReadMsg(t, pool, defaultReadID, 0, payload, nil, true, 8)}
				sr, _ = newTestBidiReader(t, msgs, int64(len(payload)), false, 0)
			}

			// Read only 5 bytes out of 36, leaving currMsg partially unread.
			buf := make([]byte, 5)
			slices, n, rel, err := sr.ReadVectored(buf)
			if err != nil || n != 5 || slices == nil || rel == nil {
				t.Fatalf("ReadVectored(5) = (%v, %d, rel!=nil:%v, %v)", slices, n, rel != nil, err)
			}
			rel()

			// Close mid-message: unread currMsg.databufs must be freed.
			if err := sr.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Second Close must be a safe no-op (no double-free).
			if err := sr.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			if active := pool.active.Load(); active != 0 {
				t.Errorf("leaked %d buffers after Close mid-message", active)
			}
		})
	}
}

func TestReadVectored_Fallbacks(t *testing.T) {
	payload1 := []byte("hello-world-chunk-1-")
	payload2 := []byte("hello-world-chunk-2!")
	wantAll := append(append([]byte(nil), payload1...), payload2...)
	fullCRC := crc32.Checksum(wantAll, crc32cTable)
	chunk2CRC := crc32.Checksum(payload2, crc32cTable)

	for _, mode := range []string{"ReadObject", "BidiReadObject"} {
		t.Run(mode+"_InitialCheckCRC", func(t *testing.T) {
			pool := &trackingBufferPool{}
			var sr *Reader
			if mode == "ReadObject" {
				msgs := []mem.BufferSlice{
					makeReadObjectMsg(t, pool, payload1, nil, 0),
					makeReadObjectMsg(t, pool, payload2, nil, 0),
				}
				sr, _ = newTestReadObjectReader(t, msgs, int64(len(wantAll)), true, fullCRC)
			} else {
				msgs := []mem.BufferSlice{
					makeBidiReadMsg(t, pool, defaultReadID, 0, payload1, nil, false, 0),
					makeBidiReadMsg(t, pool, defaultReadID, int64(len(payload1)), payload2, nil, true, 0),
				}
				sr, _ = newTestBidiReader(t, msgs, int64(len(wantAll)), true, fullCRC)
			}

			buf := make([]byte, len(wantAll))
			slices, n, rel, err := sr.ReadVectored(buf)
			if err != nil {
				t.Fatalf("ReadVectored: %v", err)
			}
			if slices != nil || rel != nil {
				t.Fatalf("expected fallback (slices=nil, rel=nil) when checkCRC=true, got slices=%v", slices)
			}
			if n != len(wantAll) || !bytes.Equal(buf[:n], wantAll) {
				t.Errorf("buf = %q, want %q", buf[:n], wantAll)
			}
			_ = sr.Close()
			if active := pool.active.Load(); active != 0 {
				t.Errorf("active buffers = %d, want 0", active)
			}
		})

		t.Run(mode+"_MidReadChunkCRC", func(t *testing.T) {
			pool := &trackingBufferPool{}
			var sr *Reader
			// Message 1 has no CRC (zero-copy), Message 2 has per-chunk CRC (triggers mid-read fallback).
			if mode == "ReadObject" {
				msgs := []mem.BufferSlice{
					makeReadObjectMsg(t, pool, payload1, nil, 0),
					makeReadObjectMsg(t, pool, payload2, &chunk2CRC, 0),
				}
				sr, _ = newTestReadObjectReader(t, msgs, int64(len(wantAll)), false, 0)
			} else {
				msgs := []mem.BufferSlice{
					makeBidiReadMsg(t, pool, defaultReadID, 0, payload1, nil, false, 0),
					makeBidiReadMsg(t, pool, defaultReadID, int64(len(payload1)), payload2, &chunk2CRC, true, 0),
				}
				sr, _ = newTestBidiReader(t, msgs, int64(len(wantAll)), false, 0)
			}

			buf := make([]byte, len(wantAll))
			slices, n, rel, err := sr.ReadVectored(buf)
			if err != nil {
				t.Fatalf("ReadVectored: %v", err)
			}
			if n != len(wantAll) || slices == nil || rel == nil {
				t.Fatalf("expected hybrid slices on mid-read CRC transition, got n=%d, slices=%v", n, slices)
			}
			assertCappedSlices(t, slices)
			if got := flattenSlices(slices); !bytes.Equal(got, wantAll) {
				t.Errorf("flattened = %q, want %q", got, wantAll)
			}
			rel()
			_ = sr.Close()
			if active := pool.active.Load(); active != 0 {
				t.Errorf("active buffers = %d, want 0", active)
			}
		})

		t.Run(mode+"_FirstMessageChunkCRC", func(t *testing.T) {
			pool := &trackingBufferPool{}
			chunk1CRC := crc32.Checksum(payload1, crc32cTable)
			var sr *Reader
			if mode == "ReadObject" {
				msgs := []mem.BufferSlice{makeReadObjectMsg(t, pool, payload1, &chunk1CRC, 0)}
				sr, _ = newTestReadObjectReader(t, msgs, int64(len(payload1)), false, 0)
			} else {
				msgs := []mem.BufferSlice{makeBidiReadMsg(t, pool, defaultReadID, 0, payload1, &chunk1CRC, true, 0)}
				sr, _ = newTestBidiReader(t, msgs, int64(len(payload1)), false, 0)
			}

			buf := make([]byte, len(payload1))
			slices, n, rel, err := sr.ReadVectored(buf)
			if err != nil {
				t.Fatalf("ReadVectored: %v", err)
			}
			if slices != nil || rel != nil {
				t.Fatalf("expected fallback (slices=nil, rel=nil) when first recv sets chunkCRCPresent")
			}
			if n != len(payload1) || !bytes.Equal(buf[:n], payload1) {
				t.Errorf("got %q, want %q", buf[:n], payload1)
			}
			_ = sr.Close()
			if active := pool.active.Load(); active != 0 {
				t.Errorf("active buffers = %d, want 0", active)
			}
		})

		t.Run(mode+"_LeftoversFallback", func(t *testing.T) {
			pool := &trackingBufferPool{}
			var sr *Reader
			if mode == "ReadObject" {
				msgs := []mem.BufferSlice{
					makeReadObjectMsg(t, pool, payload1, nil, 0),
					makeReadObjectMsg(t, pool, payload2, nil, 0),
				}
				var gr *gRPCReadObjectReader
				sr, gr = newTestReadObjectReader(t, msgs, int64(len(wantAll)), false, 0)
				gr.leftovers = []byte{1}
			} else {
				msgs := []mem.BufferSlice{
					makeBidiReadMsg(t, pool, defaultReadID, 0, payload1, nil, false, 0),
					makeBidiReadMsg(t, pool, defaultReadID, int64(len(payload1)), payload2, nil, true, 0),
				}
				var gr *gRPCReader
				sr, gr = newTestBidiReader(t, msgs, int64(len(wantAll)), false, 0)
				gr.leftovers = []byte{1}
			}

			buf := make([]byte, len(wantAll))
			slices, n, rel, err := sr.ReadVectored(buf)
			if err != nil {
				t.Fatalf("ReadVectored: %v", err)
			}
			if slices != nil || rel != nil {
				t.Fatalf("expected fallback when leftovers > 0")
			}
			if n != len(wantAll) || !bytes.Equal(buf[:n], wantAll) {
				t.Errorf("got %q, want %q", buf[:n], wantAll)
			}
			_ = sr.Close()
			if active := pool.active.Load(); active != 0 {
				t.Errorf("active buffers = %d, want 0", active)
			}
		})
	}
}

type fakeNonGRPCReader struct {
	data io.Reader
}

func (f *fakeNonGRPCReader) Read(p []byte) (int, error) {
	return f.data.Read(p)
}

func (f *fakeNonGRPCReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, f.data)
}

func (f *fakeNonGRPCReader) Close() error {
	return nil
}

func TestReadVectored_NonGRPCReaderAndRemainAccounting(t *testing.T) {
	payload := []byte("non-grpc-reader-payload")
	sr := &Reader{
		Attrs:  ReaderObjectAttrs{Size: int64(len(payload))},
		remain: int64(len(payload)),
		reader: &fakeNonGRPCReader{data: bytes.NewReader(payload)},
	}

	// Request more bytes than available to verify io.ErrUnexpectedEOF -> io.EOF normalization.
	buf := make([]byte, len(payload)+10)
	slices, n, rel, err := sr.ReadVectored(buf)
	if err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if slices != nil || rel != nil {
		t.Fatalf("expected slices=nil, rel=nil for non-gRPC reader")
	}
	if n != len(payload) || !bytes.Equal(buf[:n], payload) {
		t.Errorf("got %q (n=%d), want %q", buf[:n], n, payload)
	}
	if sr.Remain() != 0 {
		t.Errorf("Remain() = %d, want 0", sr.Remain())
	}

	// Verify unfinalized / Decompressed objects keep remain == -1 while updating bytesRead.
	pool := &trackingBufferPool{}
	msgs := []mem.BufferSlice{makeBidiReadMsg(t, pool, defaultReadID, 0, payload, nil, true, 0)}
	srUnfin, _ := newTestBidiReader(t, msgs, int64(len(payload)), false, 0)
	srUnfin.unfinalized = true
	srUnfin.remain = -1

	buf2 := make([]byte, len(payload))
	slices, n, rel, err = srUnfin.ReadVectored(buf2)
	if err != nil || n != len(payload) || slices == nil || rel == nil {
		t.Fatalf("unfinalized ReadVectored = (%v, %d, rel!=nil:%v, %v)", slices, n, rel != nil, err)
	}
	if srUnfin.Remain() != -1 {
		t.Errorf("unfinalized Remain() = %d, want -1", srUnfin.Remain())
	}
	if srUnfin.bytesRead != int64(len(payload)) {
		t.Errorf("unfinalized bytesRead = %d, want %d", srUnfin.bytesRead, len(payload))
	}
	rel()
	_ = srUnfin.Close()
}

type testZeroCopyWriter struct {
	mu     sync.Mutex
	slices [][]byte
	bufs   []mem.Buffer
}

func (w *testZeroCopyWriter) Write(p []byte) (int, error) {
	panic("unexpected call to Write when ZeroCopyWriter is implemented")
}

func (w *testZeroCopyWriter) WriteZeroCopy(p []byte, buf mem.Buffer) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) != cap(p) {
		panic("WriteZeroCopy received slice with len != cap")
	}
	buf.Ref()
	w.slices = append(w.slices, p)
	w.bufs = append(w.bufs, buf)
	return len(p), nil
}

func (w *testZeroCopyWriter) releaseAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.bufs {
		w.bufs[i].Free()
		w.bufs[i] = nil
	}
	w.bufs = nil
}

func TestZeroCopyWriter_DecoderAndMRD(t *testing.T) {
	payload1 := []byte("mrd-range-1-payload-bytes")
	payload2 := []byte("mrd-range-2-payload-bytes-extra")
	crc1 := crc32.Checksum(payload1, crc32cTable)
	crc2 := crc32.Checksum(payload2, crc32cTable)

	t.Run("readResponseDecoder_writeToAndUpdateCRC", func(t *testing.T) {
		pool := &trackingBufferPool{}
		msg := &storagepb.BidiReadObjectResponse{
			ObjectDataRanges: []*storagepb.ObjectRangeData{
				{
					ChecksummedData: &storagepb.ChecksummedData{Content: payload1, Crc32C: &crc1},
					ReadRange:       &storagepb.ReadRange{ReadId: 1, ReadOffset: 0},
					RangeEnd:        true,
				},
				{
					ChecksummedData: &storagepb.ChecksummedData{Content: payload2, Crc32C: &crc2},
					ReadRange:       &storagepb.ReadRange{ReadId: 2, ReadOffset: 100},
					RangeEnd:        true,
				},
			},
		}
		databufs := splitMarshal(t, pool, msg, 9)
		decoder := &readResponseDecoder{databufs: databufs}
		if err := decoder.readFullObjectResponse(); err != nil {
			t.Fatalf("readFullObjectResponse: %v", err)
		}
		decoder.verifyChecksums()
		if len(decoder.crcErrs) != 0 {
			t.Fatalf("unexpected crcErrs: %v", decoder.crcErrs)
		}

		zcw1 := &testZeroCopyWriter{}
		zcw2 := &testZeroCopyWriter{}
		var runningCRC1 uint32
		n1, found1, err1 := decoder.writeToAndUpdateCRC(zcw1, 1, func(b []byte) {
			runningCRC1 = crc32.Update(runningCRC1, crc32cTable, b)
		})
		if err1 != nil || !found1 || n1 != int64(len(payload1)) {
			t.Fatalf("range 1 writeToAndUpdateCRC = (%d, %v, %v)", n1, found1, err1)
		}
		if runningCRC1 != crc1 {
			t.Errorf("runningCRC1 = %d, want %d", runningCRC1, crc1)
		}

		n2, found2, err2 := decoder.writeToAndUpdateCRC(zcw2, 2, nil)
		if err2 != nil || !found2 || n2 != int64(len(payload2)) {
			t.Fatalf("range 2 writeToAndUpdateCRC = (%d, %v, %v)", n2, found2, err2)
		}

		// Simulate MultiRangeDownloader freeing decoder.databufs right after writeToAndUpdateCRC.
		decoder.databufs.Free()
		decoder.databufs = nil

		// Because zcw1 and zcw2 called buf.Ref() inside WriteZeroCopy, the underlying
		// buffers must remain alive and readable even after decoder.databufs.Free()!
		if active := pool.active.Load(); active == 0 {
			t.Fatalf("expected buffers to remain active while held by ZeroCopyWriter")
		}
		if got1 := flattenSlices(zcw1.slices); !bytes.Equal(got1, payload1) {
			t.Errorf("zcw1 data = %q, want %q", got1, payload1)
		}
		if got2 := flattenSlices(zcw2.slices); !bytes.Equal(got2, payload2) {
			t.Errorf("zcw2 data = %q, want %q", got2, payload2)
		}

		// Now release the pinned buffers from zcw1 and zcw2.
		zcw1.releaseAll()
		zcw2.releaseAll()
		if active := pool.active.Load(); active != 0 {
			t.Errorf("active buffers after zcw release = %d, want 0", active)
		}
	})

	t.Run("readObjectResponseDecoder_writeToAndUpdateCRC", func(t *testing.T) {
		pool := &trackingBufferPool{}
		databufs := makeReadObjectMsg(t, pool, payload1, &crc1, 6)
		decoder := &readObjectResponseDecoder{databufs: databufs}
		if err := decoder.readFullObjectResponse(); err != nil {
			t.Fatalf("readFullObjectResponse: %v", err)
		}

		zcw := &testZeroCopyWriter{}
		var gotCRC uint32
		n, err := decoder.writeToAndUpdateCRC(zcw, func(b []byte) {
			gotCRC = crc32.Update(gotCRC, crc32cTable, b)
		})
		if err != nil || n != int64(len(payload1)) {
			t.Fatalf("writeToAndUpdateCRC = (%d, %v)", n, err)
		}
		if gotCRC != crc1 {
			t.Errorf("gotCRC = %d, want %d", gotCRC, crc1)
		}
		// writeToAndUpdateCRC already freed decoder.databufs when reaching endBuf,
		// so only zcw's Ref() keeps the buffers alive.
		if active := pool.active.Load(); active == 0 {
			t.Fatalf("expected buffers to stay alive via zcw Ref()")
		}
		if got := flattenSlices(zcw.slices); !bytes.Equal(got, payload1) {
			t.Errorf("got %q, want %q", got, payload1)
		}
		zcw.releaseAll()
		if active := pool.active.Load(); active != 0 {
			t.Errorf("active buffers = %d, want 0", active)
		}
	})

	t.Run("MultiRangeDownloader_processSessionResult", func(t *testing.T) {
		pool := &trackingBufferPool{}
		msg := &storagepb.BidiReadObjectResponse{
			ObjectDataRanges: []*storagepb.ObjectRangeData{
				{
					ChecksummedData: &storagepb.ChecksummedData{Content: payload1, Crc32C: &crc1},
					ReadRange:       &storagepb.ReadRange{ReadId: 1, ReadOffset: 42},
					RangeEnd:        true,
				},
			},
		}
		databufs := splitMarshal(t, pool, msg, 8)
		decoder := &readResponseDecoder{databufs: databufs}
		if err := decoder.readFullObjectResponse(); err != nil {
			t.Fatalf("readFullObjectResponse: %v", err)
		}
		decoder.verifyChecksums()

		zcw := &testZeroCopyWriter{}
		var cbOffset, cbLength int64
		var cbErr error
		var cbCalled bool

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		params := &newMultiRangeDownloaderParams{bucket: "b", object: "o"}
		params.defaults()
		mgr := &multiRangeDownloaderManager{
			ctx:            ctx,
			cancel:         cancel,
			params:         params,
			readSpec:       &storagepb.BidiReadObjectSpec{},
			attrsReady:     make(chan struct{}),
			unsentRequests: newRequestQueue(),
			streams: map[int]*mrdStream{
				0: {
					id: 0,
					pendingRanges: map[int64]*rangeRequest{
						1: {
							output:     zcw,
							origOffset: 42,
							offset:     42,
							length:     int64(len(payload1)),
							readID:     1,
							callback: func(off, n int64, err error) {
								cbOffset = off
								cbLength = n
								cbErr = err
								cbCalled = true
							},
						},
					},
				},
			},
		}

		// processSessionResult writes to zcw via writeToAndUpdateCRC and defers decoder.databufs.Free().
		mgr.processSessionResult(mrdSessionResult{id: 0, decoder: decoder})
		mgr.callbackWg.Wait()

		if !cbCalled || cbErr != nil || cbOffset != 42 || cbLength != int64(len(payload1)) {
			t.Fatalf("callback = (called=%v, off=%d, len=%d, err=%v)", cbCalled, cbOffset, cbLength, cbErr)
		}
		// Even after processSessionResult freed decoder.databufs, zcw's Ref() keeps buffers alive.
		if active := pool.active.Load(); active == 0 {
			t.Fatalf("expected buffers to remain active after processSessionResult while pinned by zcw")
		}
		if got := flattenSlices(zcw.slices); !bytes.Equal(got, payload1) {
			t.Errorf("zcw data = %q, want %q", got, payload1)
		}
		zcw.releaseAll()
		if active := pool.active.Load(); active != 0 {
			t.Errorf("active buffers = %d, want 0", active)
		}
	})
}
