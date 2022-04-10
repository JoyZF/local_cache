package freecache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

const HASH_ENTRY_SIZE = 16
const ENTRY_HDR_SIZE = 24

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

// entry pointer struct points to an entry in ring buffer
type entryPtr struct {
	offset   int64  // entry offset in ring buffer 在rb的偏移量
	hash16   uint16 // entries are ordered by hash16 in a slot.
	keyLen   uint16 // used to compare a key
	reserved uint32
}

// entry header struct in ring buffer, followed by key and value.
type entryHdr struct {
	accessTime uint32 // 访问时间
	expireAt   uint32 // 过期时间
	keyLen     uint16 // key 长度
	hash16     uint16
	valLen     uint32
	valCap     uint32
	deleted    bool
	slotId     uint8
	reserved   uint16
}

// a segment contains 256 slots, a slot is an array of entry pointers ordered by hash16 value
// the entry can be looked up by hash value of the key.
type segment struct {
	// 环形buf 存储数据
	rb            RingBuf // ring buffer that stores data
	segId         int
	_             uint32
	missCount     int64 // 查询未命中数
	hitCount      int64 // 查询命中数
	entryCount    int64
	totalCount    int64 // number of entries in ring buffer, including deleted entries.
	totalTime     int64 // used to calculate least recent used entry.
	timer         Timer // Timer giving current time
	totalEvacuate int64 // used for debug
	totalExpired  int64 // used for debug
	overwrites    int64 // used for debug
	touched       int64 // used for debug
	vacuumLen     int64 // up to vacuumLen, new data can be written without overwriting old data.
	//  长度
	slotLens [256]int32 // The actual length for every slot.
	// 容量
	slotCap int32 // max number of entry pointers a slot can hold.
	// 存储索引
	slotsData []entryPtr // shared by all 256 slots
}

func newSegment(bufSize int, segId int, timer Timer) (seg segment) {
	seg.rb = NewRingBuf(bufSize, 0)
	seg.segId = segId
	seg.timer = timer
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return
}

func (seg *segment) set(key, value []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > 65535 {
		return ErrLargeKey
	}
	maxKeyValLen := len(seg.rb.data)/4 - ENTRY_HDR_SIZE
	if len(key)+len(value) > maxKeyValLen {
		// Do not accept large entry.
		return ErrLargeEntry
	}
	now := seg.timer.Now()
	expireAt := uint32(0)
	// 如果设置了过期时间 需要计算过期时间
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	// 找到key对应的最小的idx
	slotId := uint8(hashVal >> 8)   // hash 右移8位是slotId
	hash16 := uint16(hashVal >> 16) // hash 右移16
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)

	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if match {
		// 发生hash冲突
		matchedPtr := &slot[idx]
		seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		originAccessTime := hdr.accessTime
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		//
		if hdr.valCap >= hdr.valLen {
			//in place overwrite
			atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
			seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
			seg.rb.WriteAt(value, matchedPtr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
			atomic.AddInt64(&seg.overwrites, 1)
			return
		}
		// avoid unnecessary memory copy.
		// 删除 走插入
		seg.delEntryPtr(slotId, slot, idx)
		match = false
		// increase capacity and limit entry len.
		for hdr.valCap < hdr.valLen {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValLen-len(key)) {
			hdr.valCap = uint32(maxKeyValLen - len(key))
		}
	} else {
		// 新元素
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		hdr.valCap = uint32(len(value))
		if hdr.valCap == 0 { // avoid infinite loop when increasing capacity.
			hdr.valCap = 1
		}
	}
	// kv 转成entry
	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(hdr.valCap)
	slotModified := seg.evacuate(entryLen, slotId, now)
	if slotModified {
		// the slot has been modified during evacuation, we need to looked up for the 'idx' again.
		// otherwise there would be index out of bound error.
		slot = seg.getSlot(slotId)
		idx, match = seg.lookup(slot, hash16, key)
		// assert(match == false)
	}
	// 写入rb的偏移量
	newOff := seg.rb.End()
	// 维护更新索引，idx 相同的时候，往后追加
	// 写数据
	seg.insertEntryPtr(slotId, hash16, newOff, idx, hdr.keyLen)
	seg.rb.Write(hdrBuf[:])
	seg.rb.Write(key)
	seg.rb.Write(value)
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen))
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.vacuumLen -= entryLen
	return
}

func (seg *segment) touch(key []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > 65535 {
		return ErrLargeKey
	}

	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}
	matchedPtr := &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	now := seg.timer.Now()
	if hdr.expireAt != 0 && hdr.expireAt <= now {
		seg.delEntryPtr(slotId, slot, idx)
		atomic.AddInt64(&seg.totalExpired, 1)
		err = ErrNotFound
		atomic.AddInt64(&seg.missCount, 1)
		return
	}

	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	originAccessTime := hdr.accessTime
	hdr.accessTime = now
	hdr.expireAt = expireAt
	//in place overwrite
	atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
	seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
	atomic.AddInt64(&seg.touched, 1)
	return
}

func (seg *segment) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	var oldHdrBuf [ENTRY_HDR_SIZE]byte
	consecutiveEvacuate := 0
	for seg.vacuumLen < entryLen {
		oldOff := seg.rb.End() + seg.vacuumLen - seg.rb.Size()
		seg.rb.ReadAt(oldHdrBuf[:], oldOff)
		oldHdr := (*entryHdr)(unsafe.Pointer(&oldHdrBuf[0]))
		oldEntryLen := ENTRY_HDR_SIZE + int64(oldHdr.keyLen) + int64(oldHdr.valCap)
		if oldHdr.deleted {
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			continue
		}
		expired := oldHdr.expireAt != 0 && oldHdr.expireAt < now
		leastRecentUsed := int64(oldHdr.accessTime)*atomic.LoadInt64(&seg.totalCount) <= atomic.LoadInt64(&seg.totalTime)
		if expired || leastRecentUsed || consecutiveEvacuate > 5 {
			seg.delEntryPtrByOffset(oldHdr.slotId, oldHdr.hash16, oldOff)
			if oldHdr.slotId == slotId {
				slotModified = true
			}
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			if expired {
				atomic.AddInt64(&seg.totalExpired, 1)
			} else {
				atomic.AddInt64(&seg.totalEvacuate, 1)
			}
		} else {
			// evacuate an old entry that has been accessed recently for better cache hit rate.
			newOff := seg.rb.Evacuate(oldOff, int(oldEntryLen))
			seg.updateEntryPtr(oldHdr.slotId, oldHdr.hash16, oldOff, newOff)
			consecutiveEvacuate++
			atomic.AddInt64(&seg.totalEvacuate, 1)
		}
	}
	return
}

func (seg *segment) get(key, buf []byte, hashVal uint64, peek bool) (value []byte, expireAt uint32, err error) {
	// 先定位该key位于哪个slot 然后找到hdr 和ptr 索引
	hdr, ptr, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	expireAt = hdr.expireAt
	if cap(buf) >= int(hdr.valLen) {
		value = buf[:hdr.valLen]
	} else {
		value = make([]byte, hdr.valLen)
	}

	seg.rb.ReadAt(value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
	if !peek {
		// 命中+1
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

// view provides zero-copy access to the element's value, without copying to
// an intermediate buffer.
func (seg *segment) view(key []byte, fn func([]byte) error, hashVal uint64, peek bool) (err error) {
	hdr, ptr, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	start := ptr.offset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	val, err := seg.rb.Slice(start, int64(hdr.valLen))
	if err != nil {
		return err
	}
	err = fn(val)
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (hdr *entryHdr, ptr *entryPtr, err error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		if !peek {
			// 没有找到 未命中+1
			atomic.AddInt64(&seg.missCount, 1)
		}
		return
	}
	ptr = &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr = (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if !peek {
		now := seg.timer.Now()
		// 数据过期
		if hdr.expireAt != 0 && hdr.expireAt <= now {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.totalExpired, 1)
			err = ErrNotFound
			atomic.AddInt64(&seg.missCount, 1)
			return
		}
		// 查询时间统计
		atomic.AddInt64(&seg.totalTime, int64(now-hdr.accessTime))
		hdr.accessTime = now
		seg.rb.WriteAt(hdrBuf[:], ptr.offset)
	}
	return hdr, ptr, err
}

func (seg *segment) del(key []byte, hashVal uint64) (affected bool) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		return false
	}
	// 删除元素
	seg.delEntryPtr(slotId, slot, idx)
	return true
}

func (seg *segment) ttl(key []byte, hashVal uint64) (timeLeft uint32, err error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}
	ptr := &slot[idx]
	now := seg.timer.Now()

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	if hdr.expireAt == 0 {
		timeLeft = 0
		return
	} else if hdr.expireAt != 0 && hdr.expireAt >= now {
		timeLeft = hdr.expireAt - now
		return
	}
	err = ErrNotFound
	return
}

func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotLens[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

func (seg *segment) updateEntryPtr(slotId uint8, hash16 uint16, oldOff, newOff int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, oldOff)
	if !match {
		return
	}
	ptr := &slot[idx]
	ptr.offset = newOff
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	// len = cap 扩容 *2
	if seg.slotLens[slotId] == seg.slotCap {
		seg.expand()
	}
	seg.slotLens[slotId]++
	atomic.AddInt64(&seg.entryCount, 1)
	slot := seg.getSlot(slotId)
	// 将后边的数据拷贝一份 然后当前位置（idx）插入
	copy(slot[idx+1:], slot[idx:])
	slot[idx].offset = offset
	slot[idx].hash16 = hash16
	slot[idx].keyLen = keyLen
}

func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, offset)
	if !match {
		return
	}
	seg.delEntryPtr(slotId, slot, idx)
}

// 删除元素
func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	offset := slot[idx].offset
	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(entryHdrBuf[:], offset)
	entryHdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))
	entryHdr.deleted = true
	seg.rb.WriteAt(entryHdrBuf[:], offset)
	copy(slot[idx:], slot[idx+1:])
	// 长度减1
	seg.slotLens[slotId]--
	// 维护entry 数量
	atomic.AddInt64(&seg.entryCount, -1)
}

// 二分查找
func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	high := len(slot)
	for idx < high {
		mid := (idx + high) >> 1
		oldEntry := &slot[mid]
		if oldEntry.hash16 < hash16 {
			idx = mid + 1
		} else {
			high = mid
		}
	}
	return
}

func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	// 找到第一个hash16的index
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			// 如果不相等 退出
			break
		}
		// 如果相等 hash冲突
		match = int(ptr.keyLen) == len(key) && seg.rb.EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = ptr.offset == offset
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) resetStatistics() {
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
}

func (seg *segment) clear() {
	bufSize := len(seg.rb.data)
	seg.rb.Reset(0)
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	for i := 0; i < len(seg.slotLens); i++ {
		seg.slotLens[i] = 0
	}

	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
	atomic.StoreInt64(&seg.entryCount, 0)
	atomic.StoreInt64(&seg.totalCount, 0)
	atomic.StoreInt64(&seg.totalTime, 0)
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap
	return seg.slotsData[slotOff : slotOff+seg.slotLens[slotId] : slotOff+seg.slotCap]
}
