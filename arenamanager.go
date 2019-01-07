package bigqueue

import (
	"errors"
	"fmt"
	"math"
	"path"
)

const (
	cArenaFileFmt        = "arena_%d.dat"
	cMaxActiveArenaCount = int(math.MaxInt32)
)

var (
	// ErrInactiveArena is returned while trying
	// to get arena not in memory
	ErrInactiveArena = errors.New("trying to access inactive arena")
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	dir              string
	conf             *bqConfig
	baseAid          int
	arenaList        []*arena
	activeArenaCount int
	index            *queueIndex
}

// newArenaManager returns a pointer to new arenaManager
func newArenaManager(dir string, conf *bqConfig, headAid, tailAid int, index *queueIndex) (
	*arenaManager, error) {

	var activeArenaCount int
	if conf.memorySize > 0 {
		activeArenaCount = (conf.memorySize / conf.arenaSize) - 1
	} else {
		activeArenaCount = cMaxActiveArenaCount
	}

	am := &arenaManager{
		dir:              dir,
		conf:             conf,
		baseAid:          headAid,
		arenaList:        make([]*arena, tailAid-headAid+1),
		activeArenaCount: activeArenaCount,
		index:            index,
	}

	// setup arenas
	for i := 0; i < min(tailAid+1-headAid, activeArenaCount); i++ {
		if _, err := am.addArena(headAid + i); err != nil {
			am.close()
			return nil, err
		}
	}

	// setup tail arena if not already
	if activeArenaCount <= tailAid-headAid {
		if _, err := am.addArena(tailAid); err != nil {
			am.close()
			return nil, err
		}
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) (*arena, error) {
	if m.arenaList[aid-m.baseAid] != nil {
		return m.arenaList[aid-m.baseAid], nil
	}

	// check if arena can be brought in memory
	headAid, _ := m.index.getHead()
	if aid < headAid+m.activeArenaCount {
		return m.addArena(aid)
	}

	return nil, ErrInactiveArena
}

// activateArenaRange brings arenas in range starting from
// startAid to endAid into memory
func (m *arenaManager) activateArenaRange(startAid, endAid int) error {
	for aid := startAid; aid <= endAid; aid++ {
		if _, err := m.getArena(aid); err != nil {
			return err
		}
	}
	return nil
}

// getMaxInmemoryAid returns max aid which is in memory
// starting from head
func (m *arenaManager) getMaxActiveAid() int {
	headAid, _ := m.index.getHead()
	tailAid, _ := m.index.getTail()
	if m.activeArenaCount == cDefaultArenaSize {
		return tailAid
	}

	return min(headAid+m.activeArenaCount-1, tailAid)
}

// add arena will create arena with give aid.
func (m *arenaManager) addArena(aid int) (*arena, error) {
	// TODO: check if arena has already been added
	if aid < len(m.arenaList) && m.arenaList[aid-m.baseAid] != nil {
		return m.arenaList[aid-m.baseAid], nil
	}

	file := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	a, err := newArena(file, m.conf.arenaSize)
	if err != nil {
		return nil, err
	}

	if len(m.arenaList) < aid-m.baseAid+1 {
		m.arenaList = append(m.arenaList, a)
	} else {
		m.arenaList[aid-m.baseAid] = a
	}

	return a, nil
}

// close unmaps all the arenas managed by arenaManager
func (m *arenaManager) close() error {
	var retErr error

	for i := 0; i < len(m.arenaList); i++ {
		if m.arenaList[i] == nil {
			continue
		}
		if err := m.arenaList[i].Unmap(); err != nil {
			retErr = err
		}
	}

	return retErr
}

// unmapArena will unmap single arena. if isStrict flag is false, it checks
// if arena can still be in memory or not. Otherwise it forcefully unmaps it
func (m *arenaManager) unmapArena(aid int, isStrict bool) error {
	// check if already unmapped
	if m.arenaList[aid-m.baseAid] == nil {
		return nil
	}

	if !isStrict {
		// check if aid can be in memory or not
		currentHead, _ := m.index.getHead()
		if aid-currentHead < m.activeArenaCount {
			return nil
		}
	}

	// unmap arena
	if err := m.arenaList[aid-m.baseAid].Unmap(); err != nil {
		return err
	}

	m.arenaList[aid-m.baseAid] = nil
	return nil
}

// unmapArenaRange unmaps arena range starting from startAid
// to endAid. if isStrict is false, it checks if any arena can
// be in memory. If yes, it does not unmaps those arenas
func (m *arenaManager) unmapArenaRange(startAid, endAid int, isStrict bool) error {
	for aid := startAid; aid <= endAid; aid++ {
		if err := m.unmapArena(aid, isStrict); err != nil {
			return err
		}
	}
	return nil
}
