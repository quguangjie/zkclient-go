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

import ()

type ZkLeader struct {
	*zkBase
	ldPath   string
	nodeName string
	lMode    bool
	children []*ZkNode
}

func NewZkLeader(serName, nName, zkhosts string) *ZkLeader {
	zkbase := newZkBase(zkhosts)
	ldpath := zkbase.getLeaderPath() + "/" + serName
	zkleader := &ZkLeader{zkBase: zkbase, ldPath: ldpath, nodeName: nName, lMode: false, children: []*ZkNode{}}
	zkbase.start()
	return zkleader
}

func (r *ZkLeader) IsLeader() bool {
	return false
}

func (r *ZkLeader) GetFollower() []*ZkNode {
	return r.children
}

func (r *ZkLeader) OnSessionExpired() {
	return
}
func (r *ZkLeader) DataChange(path string, bytes []byte) {
	return
}
func (r *ZkLeader) NodeChange(path string, eventType RegistryEvent, children []string) {
	return
}
