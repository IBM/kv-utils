/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.kvutils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Basic distributed atomic counter.
 */
public interface SharedCounter extends Closeable {

    /**
     * @return the current value of the counter
     * @throws Exception
     */
    long getCurrentCount() throws Exception;

    /**
     * Atomic increment-and-get operation
     *
     * @return the value if the counter post-increment
     * @throws Exception
     */
    long getNextCount() throws Exception;

    @Override
    default void close() throws IOException {}
}
