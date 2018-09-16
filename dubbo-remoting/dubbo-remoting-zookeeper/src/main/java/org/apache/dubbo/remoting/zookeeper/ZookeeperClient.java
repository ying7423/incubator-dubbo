/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.zookeeper;

import org.apache.dubbo.common.URL;

import java.util.List;

public interface ZookeeperClient {

    //--------数据相关方法-------

    /**
     * 创建节点
     * @param path 节点路径
     * @param ephemeral 是否临时节点
     */
    void create(String path, boolean ephemeral);

    /**
     * 删除节点
     * @param path
     */
    void delete(String path);

    List<String> getChildren(String path);

    //---------监听相关方法-------

    /**
     * 添加childListener
     * @param path 节点路径
     * @param listener 监听器
     * @return
     */
    List<String> addChildListener(String path, ChildListener listener);

    /**
     * 移除childListener
     * @param path 节点路径
     * @param listener
     */
    void removeChildListener(String path, ChildListener listener);

    /**
     * 添加stateListener
     * @param listener
     */
    void addStateListener(StateListener listener);

    /**
     * 移除stateListener
     * @param listener
     */
    void removeStateListener(StateListener listener);

    //------状态相关方法----------

    /**
     * 是否连接
     * @return
     */
    boolean isConnected();

    /**
     * 关闭
     */
    void close();

    /**
     * 获得注册中心url
     * @return
     */
    URL getUrl();

}
