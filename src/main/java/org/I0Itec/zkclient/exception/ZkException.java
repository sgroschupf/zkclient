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
package org.I0Itec.zkclient.exception;

import org.apache.zookeeper.KeeperException;

public class ZkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ZkException() {
        super();
    }

    public ZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }

    public static ZkException create(KeeperException e) {
        switch (e.code()) {
        // case DATAINCONSISTENCY:
        // return new DataInconsistencyException();
        // case CONNECTIONLOSS:
        // return new ConnectionLossException();
        case NONODE:
            return new ZkNoNodeException(e);
            // case NOAUTH:
            // return new ZkNoAuthException();
        case BADVERSION:
            return new ZkBadVersionException(e);
            // case NOCHILDRENFOREPHEMERALS:
            // return new NoChildrenForEphemeralsException();
        case NODEEXISTS:
            return new ZkNodeExistsException(e);
            // case INVALIDACL:
            // return new ZkInvalidACLException();
            // case AUTHFAILED:
            // return new AuthFailedException();
            // case NOTEMPTY:
            // return new NotEmptyException();
            // case SESSIONEXPIRED:
            // return new SessionExpiredException();
            // case INVALIDCALLBACK:
            // return new InvalidCallbackException();

        default:
            return new ZkException(e);
        }
    }
}
