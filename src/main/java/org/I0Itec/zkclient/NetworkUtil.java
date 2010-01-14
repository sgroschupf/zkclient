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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class NetworkUtil {

    public final static String OVERWRITE_HOSTNAME_SYSTEM_PROPERTY = "zkclient.hostname.overwritten";

    public static String[] getLocalHostNames() {
        final Set<String> hostNames = new HashSet<String>();
        // we add localhost to this set manually, because if the ip 127.0.0.1 is
        // configured with more than one name in the /etc/hosts, only the first
        // name
        // is returned
        hostNames.add("localhost");
        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            for (final Enumeration<NetworkInterface> ifaces = networkInterfaces; ifaces.hasMoreElements();) {
                final NetworkInterface iface = ifaces.nextElement();
                InetAddress ia = null;
                for (final Enumeration<InetAddress> ips = iface.getInetAddresses(); ips.hasMoreElements();) {
                    ia = ips.nextElement();
                    hostNames.add(ia.getCanonicalHostName());
                    hostNames.add(ipToString(ia.getAddress()));
                }
            }
        } catch (final SocketException e) {
            throw new RuntimeException("unable to retrieve host names of localhost");
        }
        return hostNames.toArray(new String[hostNames.size()]);
    }

    private static String ipToString(final byte[] bytes) {
        final StringBuffer addrStr = new StringBuffer();
        for (int cnt = 0; cnt < bytes.length; cnt++) {
            final int uByte = bytes[cnt] < 0 ? bytes[cnt] + 256 : bytes[cnt];
            addrStr.append(uByte);
            if (cnt < 3)
                addrStr.append('.');
        }
        return addrStr.toString();
    }

    public static int hostNamesInList(final String serverList, final String[] hostNames) {
        final String[] serverNames = serverList.split(",");
        for (int i = 0; i < hostNames.length; i++) {
            final String hostname = hostNames[i];
            for (int j = 0; j < serverNames.length; j++) {
                final String serverNameAndPort = serverNames[j];
                final String serverName = serverNameAndPort.split(":")[0];
                if (serverName.equalsIgnoreCase(hostname)) {
                    return j;
                }
            }
        }
        return -1;
    }

    public static boolean hostNameInArray(final String[] hostNames, final String hostName) {
        for (final String name : hostNames) {
            if (name.equalsIgnoreCase(hostName)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isPortFree(int port) {
        try {
            Socket socket = new Socket("localhost", port);
            socket.close();
            return false;
        } catch (ConnectException e) {
            return true;
        } catch (SocketException e) {
            if (e.getMessage().equals("Connection reset by peer")) {
                return true;
            }
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getLocalhostName() {
        String property = System.getProperty(OVERWRITE_HOSTNAME_SYSTEM_PROPERTY);
        if (property != null && property.trim().length() > 0) {
            return property;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            throw new RuntimeException("unable to retrieve localhost name");
        }
    }
}
