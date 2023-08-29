package io.hnsn.kaukus.utilities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class Ref<T> {
  @Getter
  @Setter
  private T value;
}
