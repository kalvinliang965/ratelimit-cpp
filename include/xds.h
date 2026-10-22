#pragma once
#include "service.h"
namespace rlc {
class XdsProvider {
 public:
  explicit XdsProvider(Service& service);
  ~XdsProvider();
  void start();
  void stop();
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}
