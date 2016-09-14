/*
 *  Author: GuangJie Qu <qgjie456@163.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License,
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package zkclient

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ZkRequest struct {
	*zkBase
	reqName  string
	fullPath string
	children []string
	lock     sync.Mutex
}

func NewZkRequest(name string, zkhosts string, ver int) *ZkRequest {
	zkbase := newZkBase(zkhosts)
	path := strings.Join([]string{zkbase.getSerRegPath(), "/", name, "/v", strconv.Itoa(ver)}, "")
	zkrequest := &ZkRequest{zkBase: zkbase, reqName: name, fullPath: path}
	zkbase.start()
	zkrequest.registeWatcher(path, zkrequest)
	return zkrequest
}

func (r *ZkRequest) Discovery() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	children, err := r.getChildAndWatch(r.fullPath)
	if err != nil {
		return false
	}
	r.children = children
	fmt.Println("Discovery")
	return true
}

func (r *ZkRequest) GetServer(id string) (bool, string, int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	size := len(r.children)
	if size == 0 {
		return false, "", 0
	}
	index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(size)
	split := strings.Split(r.children[index], ":")
	if len(split) != 2 {
		return false, "", 0
	}
	ip := split[0]
	port, err := strconv.Atoi(split[1])
	if err != nil {
		return false, "", 0
	}
	return true, ip, port
}

func (r *ZkRequest) OnSessionExpired() {
	return
}
func (r *ZkRequest) DataChange(path string, bytes []byte) {
	return
}
func (r *ZkRequest) NodeChange(path string, eventType RegistryEvent, children []string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	fmt.Println("NodeChange")
	r.children = children
	return
}
