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
	"strconv"
	"strings"
)

type ZkAccept struct {
	*ZkLeader
	acPath   string
	nodeName string
}

func NewZkAccept(serName, nName, zkhosts string, ver int) *ZkAccept {
	leader := NewZkLeader(serName, nName, zkhosts)
	path := strings.Join([]string{leader.getSerRegPath(), "/", serName, "/v", strconv.Itoa(ver)}, "")
	zkaccept := &ZkAccept{ZkLeader: leader, acPath: path, nodeName: nName}
	zkaccept.createPersistentPath(path)
	return zkaccept
}

func (self *ZkAccept) Register() bool {
	path := strings.Join([]string{self.acPath, "/", self.nodeName}, "")
	self.deletePath(path)
	self.registeWatcher(path, self)
	self.registeServer(self.acPath, self.nodeName, nil)
	return true
}

func (self *ZkAccept) OnSessionExpired() {
	fmt.Println("OnSessionExpired")
	return
}
func (self *ZkAccept) DataChange(path string, bytes []byte) {
	//fmt.Println("DataChange", path)
	return
}
func (self *ZkAccept) NodeChange(path string, eventType RegistryEvent, children []string) {
	if eventType == Deleted {
		self.registeServer(self.acPath, self.nodeName, nil)
	}
	return
}
