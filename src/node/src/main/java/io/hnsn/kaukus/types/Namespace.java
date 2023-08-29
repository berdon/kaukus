package io.hnsn.kaukus.types;

import io.hnsn.kaukus.utilities.Ref;
import java.util.Objects;

/**
 * Segmentation qualifier to provide isolation around key/value pairs.
 */
public class Namespace {
  private final String namespace;

  private Namespace(String namespace) {
    this.namespace = namespace;
  }

  public static boolean tryParse(String namespace, Ref<Namespace> out) {
    if (!namespace.matches("[a-zA-Z0-9\\-_]*")) {
      return false;
    }

    out.setValue(new Namespace(namespace));
    return true;
  }

  @Override
  public String toString() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final var namespace1 = (Namespace) o;
    return namespace.equals(namespace1.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace);
  }
}
