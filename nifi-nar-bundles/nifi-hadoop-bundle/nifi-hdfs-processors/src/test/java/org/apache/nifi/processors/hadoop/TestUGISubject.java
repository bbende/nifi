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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.mockito.Mockito;

import java.security.PrivilegedExceptionAction;

public class TestUGISubject {

    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        final String principal = "bbende@CFM.COM";
        final String keytab = "/Users/bbende/Projects/docker-kdc/krb5.keytab";

        final KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);

        final PrivilegedExceptionAction<UserGroupInformation> privilegedAction = () -> {
            return UserGroupInformation.getCurrentUser();
        };

        final ComponentLog componentLog = Mockito.mock(ComponentLog.class);

        final KerberosAction<UserGroupInformation> kerberosAction = new KerberosAction<>(kerberosUser, privilegedAction, componentLog);
        final UserGroupInformation currentUser = kerberosAction.execute();
        System.out.println(currentUser.getUserName());
    }
}
