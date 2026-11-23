package shm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	headerSize     = 64
	slotHeaderSize = 64
)

// Writer writes assembled video frames into a POSIX /dev/shm ring buffer compatible with Rust ShmReader.
type Writer struct {
	mu          sync.Mutex
	file        *os.File
	filePath    string
	mmapBuf     []byte
	slotCount   uint32
	slotSize    uint32
	maxPayload  uint32
	frameSeq    uint64
	notifyPtr   *uint32
	writeSeqPtr *uint64
}

// OpenWriter creates or truncates /dev/shm/hydra_<streamID> and maps it for zero-copy frame publishing.
func (m *Manager) OpenWriter(streamID string, slotCount, maxPayloadBytes uint32) (*Writer, error) {
	if slotCount == 0 {
		slotCount = 16
	}
	if maxPayloadBytes == 0 {
		maxPayloadBytes = 512 * 1024
	}
	slotSize := slotHeaderSize + maxPayloadBytes
	totalSize := int64(headerSize) + int64(slotCount)*int64(slotSize)
	filePath := filepath.Join(m.baseDir, fmt.Sprintf("hydra_%s", streamID))

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(totalSize); err != nil {
		_ = f.Close()
		_ = os.Remove(filePath)
		return nil, err
	}

	mmapBuf, err := unix.Mmap(int(f.Fd()), 0, int(totalSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(filePath)
		return nil, err
	}

	// Write 64-byte global ShmHeader in little-endian layout matching Rust #[repr(C, align(64))]
	binary.LittleEndian.PutUint32(mmapBuf[0:4], HydraMagic)
	binary.LittleEndian.PutUint32(mmapBuf[4:8], HydraVersion)
	binary.LittleEndian.PutUint32(mmapBuf[8:12], 1920)
	binary.LittleEndian.PutUint32(mmapBuf[12:16], 1080)
	binary.LittleEndian.PutUint32(mmapBuf[16:20], 1)
	binary.LittleEndian.PutUint32(mmapBuf[20:24], slotCount)
	binary.LittleEndian.PutUint32(mmapBuf[24:28], slotSize)
	binary.LittleEndian.PutUint32(mmapBuf[28:32], 0)
	binary.LittleEndian.PutUint64(mmapBuf[32:40], 0)

	return &Writer{
		file:        f,
		filePath:    filePath,
		mmapBuf:     mmapBuf,
		slotCount:   slotCount,
		slotSize:    slotSize,
		maxPayload:  maxPayloadBytes,
		notifyPtr:   (*uint32)(unsafe.Pointer(&mmapBuf[28])),
		writeSeqPtr: (*uint64)(unsafe.Pointer(&mmapBuf[32])),
	}, nil
}

// WriteFrame writes a frame into the next ring slot and wakes sleeping Rust Futex readers.
func (w *Writer) WriteFrame(timestampUs uint64, isKeyframe bool, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mmapBuf == nil || len(data) == 0 || uint32(len(data)) > w.maxPayload {
		return nil
	}

	w.frameSeq++
	seq := w.frameSeq
	slotIdx := uint32(seq % uint64(w.slotCount))
	offset := headerSize + int(slotIdx)*int(w.slotSize)

	var flags uint32
	if isKeyframe {
		flags = 1
	}

	binary.LittleEndian.PutUint64(w.mmapBuf[offset:offset+8], seq)
	binary.LittleEndian.PutUint64(w.mmapBuf[offset+8:offset+16], timestampUs)
	binary.LittleEndian.PutUint64(w.mmapBuf[offset+16:offset+24], seq)
	binary.LittleEndian.PutUint32(w.mmapBuf[offset+24:offset+28], uint32(len(data)))
	binary.LittleEndian.PutUint32(w.mmapBuf[offset+28:offset+32], flags)
	copy(w.mmapBuf[offset+slotHeaderSize:], data)

	atomic.StoreUint64(w.writeSeqPtr, seq)
	atomic.AddUint32(w.notifyPtr, 1)
	_, _, _ = unix.Syscall(unix.SYS_FUTEX, uintptr(unsafe.Pointer(w.notifyPtr)), 1, 0x7fffffff)
	return nil
}

// Close unmaps and removes the /dev/shm ring buffer file.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mmapBuf != nil {
		_ = unix.Munmap(w.mmapBuf)
		w.mmapBuf = nil
	}
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.filePath != "" {
		_ = os.Remove(w.filePath)
		w.filePath = ""
	}
}
