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
	"sort"
	"strings"
)

type ZkLeader struct {
	*zkBase
	ldPath   string
	nodeName string
	lMode    bool
	children []*ZkNode
}

func NewZkLeader(serName, nName, zkhosts string) *ZkLeader {
	zkbase := newZkBase(zkhosts)
	path := strings.Join([]string{zkbase.getLeaderPath(), "/", serName}, "")
	zkleader := &ZkLeader{zkBase: zkbase, ldPath: path, nodeName: nName, lMode: false, children: []*ZkNode{}}
	zkbase.start()
	return zkleader
}
func DeleteZkLeader(l *ZkLeader) {
	if l.lMode {
		fullpath := strings.Join([]string{l.ldPath, "/", l.nodeName}, "")
		l.deletePath(fullpath)
	}
}

func (self *ZkLeader) IsLeader() bool {
	children, err := self.getChildAndWatch(self.ldPath)
	if err != nil {
		return false
	}
	sort.Strings(children)
	return false
}

func (self *ZkLeader) GetFollower() (bool, []*ZkNode) {
	children, err := self.getChildAndWatch(self.ldPath)
	if err != nil {
		return false, nil
	}
	sort.Strings(children)
	return true, self.children
}

func (self *ZkLeader) OnSessionExpired() {
	return
}
func (self *ZkLeader) DataChange(path string, bytes []byte) {
	return
}
func (self *ZkLeader) NodeChange(path string, eventType RegistryEvent, children []string) {
	return
}
