package bigqueue

// Enqueue adds a new element to the tail of the queue
func (bq *BigQueue) Enqueue(message []byte) error {
	aid, offset := bq.index.getTail()

	aid, offset, err := bq.writeLength(aid, offset, uint64(len(message)))
	if err != nil {
		return err
	}

	// write message
	aid, offset, err = bq.writeBytes(aid, offset, message)
	if err != nil {
		return err
	}

	// update tail
	bq.index.putTail(aid, offset)

	return nil
}

func (bq *BigQueue) writeLength(aid, offset int, length uint64) (int, int, error) {
	// ensure that new arena is available if needed
	if cInt64Size+offset > bq.conf.arenaSize {
		if err := bq.getNextArena(aid); err != nil {
			return 0, 0, err
		}
		aid, offset = aid+1, 0
	}

	arena, err := bq.am.getArena(aid)
	if err != nil {
		return 0, 0, err
	}
	arena.WriteUint64(offset, length)

	// update offset now
	offset += cInt64Size
	if offset == bq.conf.arenaSize {
		if err := bq.getNextArena(aid); err != nil {
			return 0, 0, err
		}
		aid, offset = aid+1, 0
	}

	return aid, offset, nil
}

// writeBytes writes byteSlice in arena with aid starting at offset
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (int, int, error) {
	length := len(byteSlice)
	counter := 0
	for {
		arena, err := bq.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}
		bytesWritten, err := arena.Write(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, err
		}
		counter += bytesWritten
		offset += bytesWritten

		// ensure the next arena is available if needed
		if offset == bq.conf.arenaSize {
			if err := bq.getNextArena(aid); err != nil {
				return 0, 0, err
			}

			aid, offset = aid+1, 0
		}

		// check if all bytes are written
		if counter == length {
			break
		}
	}

	return aid, offset, nil
}

// getNextArena gets arena with arena ID prevAid+1.
// It also unmap arena with arena ID prevAid
func (bq *BigQueue) getNextArena(prevAid int) error {
	if err := bq.am.unmapArena(prevAid, false); err != nil {
		return err
	}

	if _, err := bq.am.addArena(prevAid + 1); err != nil {
		return err
	}

	return nil
}
