/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ZkLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;

    private Condition _dataChangedCondition = newCondition();
    private Condition _stateChangedCondition = newCondition();
    private Condition _zNodeEventCondition = newCondition();

    /**
     * This condition will be signaled if a zookeeper event was processed and the event contains a data/child change.
     * 
     * @return the condition.
     */
    public Condition getDataChangedCondition() {
        return _dataChangedCondition;
    }

    /**
     * This condition will be signaled if a zookeeper event was processed and the event contains a state change
     * (connected, disconnected, session expired, etc ...).
     * 
     * @return the condition.
     */
    public Condition getStateChangedCondition() {
        return _stateChangedCondition;
    }

    /**
     * This condition will be signaled if any znode related zookeeper event was received.
     * 
     * @return the condition.
     */
    public Condition getZNodeEventCondition() {
        return _zNodeEventCondition;
    }
}