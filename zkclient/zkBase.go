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
	"github.com/blackbeans/go-zookeeper/zk"
	log "github.com/blackbeans/log4go"
	_ "net"
	"strings"
	"time"
)

type zkBase struct {
	zkhosts   string
	wathcers  map[string]IWatcher //基本的路径--->watcher zk可以复用了
	session   *zk.Conn
	eventChan <-chan zk.Event
	isClose   bool
}

func newZkBase(zkhosts string) *zkBase {
	zkbase := &zkBase{zkhosts: zkhosts, wathcers: make(map[string]IWatcher, 10)}
	return zkbase
}

func (self *zkBase) getSerRegPath() string {
	return serRegPath
}

func (self *zkBase) getDLockPath() string {
	return serDLockPath
}

func (self *zkBase) getLeaderPath() string {
	return serLeaderPath
}

func (self *zkBase) start() {
	if len(self.zkhosts) <= 0 {
		log.WarnLog("zkBase", "使用默认zkhosts！|localhost:2181\n")
		self.zkhosts = "localhost:2181"
	} else {
		log.Info("使用zkhosts:[%s]！\n", self.zkhosts)
	}

	ss, eventChan, err := zk.Connect(strings.Split(self.zkhosts, ","), 5*time.Second)
	if nil != err {
		panic("连接zk失败..." + err.Error())
		return
	}

	self.session = ss
	self.isClose = false
	self.eventChan = eventChan

	self.createPersistentPath(serRegPath)
	self.createPersistentPath(serDLockPath)
	self.createPersistentPath(serLeaderPath)
	go self.listenEvent()
}

//如果返回false则已经存在
func (self *zkBase) registeWatcher(rootpath string, w IWatcher) bool {
	_, ok := self.wathcers[rootpath]

	log.InfoLog("zkBase", "registeWatcher|%s", rootpath)
	if ok {
		return false
	} else {
		self.wathcers[rootpath] = w
		return true
	}
}

//监听数据变更
func (self *zkBase) listenEvent() {
	for !self.isClose {

		//根据zk的文档 Watcher机制是无法保证可靠的，其次需要在每次处理完Watcher后要重新注册Watcher
		change := <-self.eventChan
		path := change.Path
		//开始检查符合的watcher
		watcher := func() IWatcher {
			for k, w := range self.wathcers {
				//以给定的
				if strings.Index(path, k) >= 0 {

					return w
				}
			}
			return nil
		}()

		//如果没有wacher那么久忽略
		if nil == watcher {
			log.WarnLog("zkBase", "zkBase|listenEvent|NO  WATCHER|%s", path)
			continue
		}

		switch change.Type {
		case zk.EventSession:
			if change.State == zk.StateExpired ||
				change.State == zk.StateDisconnected {
				log.WarnLog("zkBase", "zkBase|OnSessionExpired!|Reconnect Zk ....")
				//阻塞等待重连任务成功
				succ := <-self.reconnect()
				if !succ {
					log.WarnLog("zkBase", "zkBase|OnSessionExpired|Reconnect Zk|FAIL| ....")
					continue
				}

				//session失效必须通知所有的watcher
				func() {
					for _, w := range self.wathcers {
						//zk链接开则需要重新链接重新推送
						w.OnSessionExpired()
					}
				}()

			}
		case zk.EventNodeDeleted:
			self.session.ExistsW(path)
			watcher.NodeChange(path, RegistryEvent(change.Type), []string{})
			// log.Info("zkBase|listenEvent|%s|%s\n", path, change)
		case zk.EventNodeCreated, zk.EventNodeChildrenChanged:
			childnodes, _, _, err := self.session.ChildrenW(path)
			if nil != err {
				log.ErrorLog("zkBase", "zkBase|listenEvent|CD|%s|%s|%t\n", err, path, change.Type)
			} else {
				watcher.NodeChange(path, RegistryEvent(change.Type), childnodes)
				log.Info("zkBase|listenEvent|%s|%s|%s\n", path, change, childnodes)
			}

		case zk.EventNodeDataChanged:
			split := strings.Split(path, "/")
			//如果不是bind级别的变更则忽略
			if len(split) < 5 || strings.LastIndex(split[4], "-bind") <= 0 {
				continue
			}
			//获取一下数据
			data, err := self.getData(path)
			if nil != err {
				log.ErrorLog("zkBase", "zkBase|listenEvent|Changed|Get DATA|FAIL|%s|%s\n", err, path)
				//忽略
				continue
			}
			watcher.DataChange(path, data)
			log.Info("zkBase|listenEvent|%s|%s|%s\n", path, change, data)
		}
	}
}

/*
*重连zk
 */
func (self *zkBase) reconnect() <-chan bool {
	ch := make(chan bool, 1)
	go func() {

		reconnTimes := int64(0)
		f := func(times time.Duration) error {
			ss, eventChan, err := zk.Connect(strings.Split(self.zkhosts, ","), 5*time.Second)
			if nil != err {
				log.WarnLog("zkBase", "Zk Reconneting FAIL|%ds", times.Seconds())
				return err
			} else {
				log.InfoLog("zkBase", "Zk Reconneting SUCC....")
				//初始化当前的状态
				self.session = ss
				self.eventChan = eventChan

				ch <- true
				close(ch)
				return nil
			}

		}
		//启动重连任务
		for !self.isClose {
			duration := time.Duration(reconnTimes * time.Second.Nanoseconds())
			select {
			case <-time.After(duration):
				err := f(duration)
				if nil != err {
					reconnTimes += 1
				} else {
					//重连成功则推出
					break
				}
			}
		}

		//失败
		ch <- false
		close(ch)
	}()
	return ch
}
func (self *zkBase) createPersistentPath(path string) {
	self.traverseCreatePath(path, nil, zk.CreatePersistent)
}
func (self *zkBase) deletePath(path string) {
	exist, stat, err := self.session.Exists(path)
	if nil != err {
		self.session.Close()
		panic("无法删除path " + err.Error())
	}
	if exist {
		err := self.session.Delete(path, stat.Version)
		if nil != err {
			self.session.Close()
			panic("NewzkBase|DELETE PATH|FAIL|" + path + "|" + err.Error())
		}
	}
	//做一下校验等待
	for i := 0; i < 5; i++ {
		exist, _, _ = self.session.Exists(path)
		if !exist {
			time.Sleep(time.Duration(i*100) * time.Millisecond)
		} else {
			break
		}
	}
}

//注册当前进程节点
func (self *zkBase) registeServer(path string, nName string, data []byte) (string, error) {
	err := self.traverseCreatePath(path, nil, zk.CreatePersistent)
	if nil == err {
		err := self.innerCreatePath(path+"/"+nName, data, zk.CreateEphemeral)
		if nil != err {
			log.ErrorLog("zkBase", "zkBase|registeServer|CREATE CHILD|FAIL|%s|%s\n", err, path+"/"+nName)
			return "", err
		} else {
			return path + "/" + nName, nil
		}
	}
	return "", err
}

func (self *zkBase) traverseCreatePath(path string, data []byte, createType zk.CreateType) error {
	split := strings.Split(path, "/")[1:]
	tmppath := "/"
	for i, v := range split {
		tmppath += v
		// log.Printf("zkBase|traverseCreatePath|%s\n", tmppath)
		if i >= len(split)-1 {
			break
		}
		err := self.innerCreatePath(tmppath, nil, zk.CreatePersistent)
		if nil != err {
			log.ErrorLog("zkBase", "zkBase|traverseCreatePath|FAIL|%s\n", err)
			return err
		}
		tmppath += "/"

	}

	//对叶子节点创建及添加数据
	return self.innerCreatePath(tmppath, data, createType)
}

//内部创建节点的方法
func (self *zkBase) innerCreatePath(tmppath string, data []byte, createType zk.CreateType) error {
	exist, _, _, err := self.session.ExistsW(tmppath)
	if nil == err && !exist {
		_, err := self.session.Create(tmppath, data, createType, zk.WorldACL(zk.PermAll))
		if nil != err {
			log.ErrorLog("zkBase", "zkBase|innerCreatePath|FAIL|%s|%s\n", err, tmppath)
			return err
		}

		//做一下校验等待
		for i := 0; i < 5; i++ {
			exist, _, _ = self.session.Exists(tmppath)
			if !exist {
				time.Sleep(time.Duration(i*100) * time.Millisecond)
			} else {
				break
			}
		}

		return err
	} else if nil != err {
		log.ErrorLog("zkBase", "zkBase|innerCreatePath|FAIL|%s\n", err)
		return err
	} else if nil != data {
		//存在该节点，推送新数据
		_, err := self.session.Set(tmppath, data, -1)
		if nil != err {
			log.ErrorLog("zkBase", "zkBase|innerCreatePath|PUSH DATA|FAIL|%s|%s|%s\n", err, tmppath, string(data))
			return err
		}
	}
	return nil
}

func (self *zkBase) getData(path string) ([]byte, error) {
	data, _, _, err := self.session.GetW(path)
	if nil != err {
		log.ErrorLog("zkBase", "zkBase|getData|Binding|FAIL|%s|%s\n", err, path)
		return nil, err
	}
	if nil == data || len(data) <= 0 {
		return []byte{}, nil
	}
	return data, nil
}

func (self *zkBase) getChildAndWatch(path string) ([]string, error) {

	exist, _, _, _ := self.session.ExistsW(path)
	if !exist {
		return []string{}, nil
	}
	//获取topic下的所有qserver
	children, _, _, err := self.session.ChildrenW(path)
	if nil != err {
		log.ErrorLog("zkBase", "GetChildAndWatch|FAIL|%s\n", path)
		return nil, err

	}
	return children, nil
}

func (self *zkBase) close() {
	self.isClose = true
	self.session.Close()
}
