package io.dpumesh.grpc;

import java.net.SocketAddress;
import java.util.Objects;

/** A DPUMesh service target, {@code "<host>:<port>"}. */
public final class DpumeshAddress extends SocketAddress {
  private static final long serialVersionUID = 1L;

  private final String service;

  public DpumeshAddress(String service) {
    if (service == null || service.isEmpty()) {
      throw new IllegalArgumentException("a DPUMesh service target is \"<host>:<port>\"");
    }
    this.service = service;
  }

  public String service() {
    return service;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DpumeshAddress && ((DpumeshAddress) other).service.equals(service);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(service);
  }

  @Override
  public String toString() {
    return service;
  }
}
