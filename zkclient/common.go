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
	"log"
	"strings"
)

const (
	serRegPath     = "/register"
	serDLockPath   = "/lock"
	serLeaderPath  = "/leader"
	serSessionPath = "/session"
)

type RegistryEvent byte

const (
	Created RegistryEvent = 1 // From Exists, Get NodeCreated (1),
	Deleted RegistryEvent = 2 // From Exists, Get	NodeDeleted (2),
	Changed RegistryEvent = 3 // From Exists, Get NodeDataChanged (3),
	Child   RegistryEvent = 4 // From Children NodeChildrenChanged (4)
)

//每个watcher
type IWatcher interface {
	//当断开链接时
	OnSessionExpired()

	DataChange(path string, bytes []byte)
	NodeChange(path string, eventType RegistryEvent, children []string)
}

type MockWatcher struct {
}

func (self *MockWatcher) OnSessionExpired() {

}

func (self *MockWatcher) DataChange(path string, bytes []byte) {

	if strings.EqualFold(path, serRegPath) {
		log.Printf("MockWatcher|DataChange|SUB节点变更|%s|%v\n", path)
	} else {
		log.Printf("MockWatcher|DataChange|非SUB节点变更|%s\n", path)
	}

	return
}

func (self *MockWatcher) NodeChange(path string, eventType RegistryEvent, childNode []string) {

	if strings.EqualFold(path, serRegPath) {
		log.Printf("MockWatcher|NodeChange|SUB节点变更||%s|%d|%v", path, eventType, childNode)
	} else {
		log.Printf("MockWatcher|NodeChange|节点变更||%s|%d|%v", path, eventType, childNode)
	}
	return
}
