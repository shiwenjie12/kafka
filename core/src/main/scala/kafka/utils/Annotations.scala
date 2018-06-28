/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import scala.annotation.StaticAnnotation

/* Some helpful annotations */

/**
 * 表示带注释的类应该是线程安全的。 对于抽象类来说，它是实现必须尊重的接口的一部分
 */
class threadsafe extends StaticAnnotation

/**
 * 指示注释的类不是线程安全的
 */
class nonthreadsafe extends StaticAnnotation

/**
 * 指示注释类是不可变的
 */
class immutable extends StaticAnnotation
