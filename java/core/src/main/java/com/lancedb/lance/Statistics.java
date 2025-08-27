/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb.lance;

import java.util.HashMap;
import java.util.Map;

public class Statistics {
  private final long rowCount;
  private final Map<Integer, Long> valueCounts;
  private final Map<Integer, Long> nullValueCounts;
  private final Map<Integer, Long> nanValueCounts;
  private final Map<Integer, Object> minValues;
  private final Map<Integer, Object> maxValues;

  // 构造函数
  public Statistics(
      long rowCount,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, Object> minValues,
      Map<Integer, Object> maxValues) {
    this.rowCount = rowCount;
    this.valueCounts = valueCounts != null ? new HashMap<>(valueCounts) : new HashMap<>();
    this.nullValueCounts =
        nullValueCounts != null ? new HashMap<>(nullValueCounts) : new HashMap<>();
    this.nanValueCounts = nanValueCounts != null ? new HashMap<>(nanValueCounts) : new HashMap<>();
    this.minValues = minValues != null ? new HashMap<>(minValues) : new HashMap<>();
    this.maxValues = maxValues != null ? new HashMap<>(maxValues) : new HashMap<>();
  }

  // Getter方法
  public long getRowCount() {
    return rowCount;
  }

  public Map<Integer, Long> getValueCounts() {
    return new HashMap<>(valueCounts);
  }

  public Map<Integer, Long> getNullValueCounts() {
    return new HashMap<>(nullValueCounts);
  }

  public Map<Integer, Long> getNanValueCounts() {
    return new HashMap<>(nanValueCounts);
  }

  public Map<Integer, Object> getMinValues() {
    return new HashMap<>(minValues);
  }

  public Map<Integer, Object> getMaxValues() {
    return new HashMap<>(maxValues);
  }
}
