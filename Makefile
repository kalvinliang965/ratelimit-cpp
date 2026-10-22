CXX ?= c++
PROTOC ?= /usr/bin/protoc
PYTHON ?= python3
BUILD ?= build
PREFIX ?= /usr/local
GO_BUILD_CACHE ?= $(CURDIR)/.cache/go-build
GO_MODULE_CACHE ?= $(CURDIR)/.cache/go-mod
LOCAL_DEPS := $(CURDIR)/.deps/usr
LOCAL_CPPFLAGS :=
LOCAL_LDFLAGS :=
ifneq ($(wildcard $(LOCAL_DEPS)/include/grpcpp/grpcpp.h),)
  GRPC_CPP_PLUGIN ?= $(LOCAL_DEPS)/bin/grpc_cpp_plugin
  LOCAL_CPPFLAGS += -I$(LOCAL_DEPS)/include
  LOCAL_LDFLAGS += -L$(LOCAL_DEPS)/lib/x86_64-linux-gnu -Wl,-rpath,$(LOCAL_DEPS)/lib/x86_64-linux-gnu
else
  GRPC_CPP_PLUGIN ?= $(shell command -v grpc_cpp_plugin)
endif
CPPFLAGS += $(LOCAL_CPPFLAGS) -Iinclude -I$(BUILD)/generated
LDFLAGS += $(LOCAL_LDFLAGS)
CXXFLAGS ?= -O2 -g
CXXFLAGS += -std=c++17 -pthread -Wall -Wextra
# Debian's gRPC package deliberately does not expose all of its Abseil
# dependencies through libgrpc++.  Prefer its pkg-config closure when it is
# present; retain the compact fallback used by the vendored local packages.
GRPC_LDLIBS := $(shell pkg-config --libs grpc++ 2>/dev/null)
ifeq ($(strip $(GRPC_LDLIBS)),)
  GRPC_LDLIBS := -lgrpc++ -lgrpc -lgpr
endif
LDLIBS += $(GRPC_LDLIBS) -lprotobuf -lyaml-cpp -lssl -lcrypto -lresolv -lpthread
PROTO_FILES := $(shell find proto -name '*.proto' | sort)
PROTO_NAMES := $(patsubst proto/%,%,$(PROTO_FILES))
PROTO_CC := $(addprefix $(BUILD)/generated/,$(PROTO_NAMES:.proto=.pb.cc))
GRPC_NAMES := envoy/service/ratelimit/v3/rls envoy/service/discovery/v3/ads grpc/health/v1/health
GRPC_CC := $(addprefix $(BUILD)/generated/,$(addsuffix .grpc.pb.cc,$(GRPC_NAMES)))
PROTO_OBJS := $(PROTO_CC:.cc=.o) $(GRPC_CC:.cc=.o)
SOURCES := $(wildcard src/*.cc)
OBJECTS := $(patsubst src/%.cc,$(BUILD)/obj/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d) $(PROTO_OBJS:.o=.d)
ASAN_BUILD ?= build-asan
TSAN_BUILD ?= build-tsan
ASAN_CXXFLAGS := -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined
ASAN_LDFLAGS := $(LOCAL_LDFLAGS) -fsanitize=address,undefined
TSAN_CXXFLAGS := -O1 -g -fno-omit-frame-pointer -fsanitize=thread
TSAN_LDFLAGS := $(LOCAL_LDFLAGS) -fsanitize=thread

.PHONY: all clean run test test-unit test-smoke test-parity test-go-reference-vectors test-integration test-load test-backend-modes backend-client-image asan test-asan tsan test-tsan python-protos install help check-deps deps docker-build docker-test
all: $(BUILD)/ratelimit-cpp
help:
	@echo 'make [-j4]           Build the standalone C++ service'
	@echo 'make test           Config unit tests and Redis/gRPC smoke tests'
	@echo 'make test-parity     Compare against ../ratelimit Go checkout (Go required)'
	@echo 'make test-go-reference-vectors Run selected unmodified Go test fixtures through real C++/Go gRPC/Redis'
	@echo 'make test-integration Run black-box Redis/gRPC/restart/failure parity checks'
	@echo 'make test-load       Run real multi-client gRPC/Redis concurrency and restart parity checks'
	@echo 'make test-backend-modes Run real Docker Redis Cluster/Sentinel C++/Go gRPC parity checks'
	@echo 'make test-asan       Build with ASan/UBSan and run the C++ load suite'
	@echo 'make test-tsan       Build with TSan and run the C++ load suite (if runtime supports it)'
	@echo 'make run            Run using environment configuration'
	@echo 'make install        Install binary under PREFIX (default /usr/local)'
	@echo 'make deps           Download unprivileged Ubuntu build dependencies into .deps/'
	@echo 'make docker-build   Build the C++ runtime and external gRPC client images cleanly'
	@echo 'make docker-test    Run the Docker Redis/gRPC/restart/Go-parity validation'
	@echo 'make clean          Remove generated build files'
check-deps:
	@command -v $(CXX) >/dev/null
	@test -x '$(PROTOC)' || { echo 'Install protobuf-compiler or set PROTOC'; exit 1; }
	@test -x '$(GRPC_CPP_PLUGIN)' || { echo 'Install protobuf-compiler-grpc or set GRPC_CPP_PLUGIN'; exit 1; }
deps:
	@mkdir -p .deps/packages .deps
	cd .deps/packages && apt-get download libgrpc++-dev libgrpc-dev libgrpc++1 libgrpc10 protobuf-compiler-grpc libhiredis-dev libhiredis0.14 libabsl-dev libre2-dev libc-ares-dev
	@for package in .deps/packages/*.deb; do dpkg-deb -x "$$package" .deps; done
$(BUILD)/generated/.stamp: $(PROTO_FILES) | check-deps
	@mkdir -p $(BUILD)/generated
	$(PROTOC) --experimental_allow_proto3_optional -Iproto --cpp_out=$(BUILD)/generated $(PROTO_FILES)
	$(PROTOC) --experimental_allow_proto3_optional -Iproto --grpc_out=$(BUILD)/generated --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN) $(addprefix proto/,$(addsuffix .proto,$(GRPC_NAMES)))
	$(PROTOC) --experimental_allow_proto3_optional -Iproto --include_imports --descriptor_set_out=$(BUILD)/api.pb $(PROTO_FILES)
	@touch $@
$(PROTO_CC) $(GRPC_CC): $(BUILD)/generated/.stamp
$(BUILD)/generated/%.o: $(BUILD)/generated/%.cc $(BUILD)/generated/.stamp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -Wno-deprecated-declarations -MMD -MP -c $< -o $@
$(BUILD)/obj/%.o: src/%.cc $(BUILD)/generated/.stamp
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@
$(BUILD)/ratelimit-cpp: $(OBJECTS) $(PROTO_OBJS)
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@
$(BUILD)/config-test: tests/config_test.cc $(BUILD)/obj/config.o $(PROTO_OBJS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@
test-unit: $(BUILD)/config-test
	./$(BUILD)/config-test
python-protos: $(BUILD)/generated/.stamp
	$(PYTHON) scripts/python_protos.py $(BUILD)/api.pb $(BUILD)/python
test-smoke: all python-protos
	$(PYTHON) tests/parity.py --cpp $(BUILD)/ratelimit-cpp --generated $(BUILD)/python
$(BUILD)/ratelimit-go:
	@mkdir -p $(BUILD)
	cd ../ratelimit && GOCACHE=$(GO_BUILD_CACHE) GOMODCACHE=$(GO_MODULE_CACHE) go build -o $(abspath $@) ./src/service_cmd
test-parity: all python-protos $(BUILD)/ratelimit-go
	$(PYTHON) tests/parity.py --cpp $(BUILD)/ratelimit-cpp --go $(BUILD)/ratelimit-go --generated $(BUILD)/python
test-go-reference-vectors: all python-protos $(BUILD)/ratelimit-go
	$(PYTHON) tests/go_reference_vectors.py --cpp $(BUILD)/ratelimit-cpp --go $(BUILD)/ratelimit-go --generated $(BUILD)/python
test-integration: all python-protos $(BUILD)/ratelimit-go
	$(PYTHON) tests/integration_validation.py --cpp $(BUILD)/ratelimit-cpp --go $(BUILD)/ratelimit-go --generated $(BUILD)/python
test-load: all python-protos $(BUILD)/ratelimit-go
	$(PYTHON) tests/load.py --cpp $(BUILD)/ratelimit-cpp --go $(BUILD)/ratelimit-go --generated $(BUILD)/python
backend-client-image:
	docker build --tag ratelimit-cpp-validation-client:latest --file docker/client.Dockerfile .
test-backend-modes: all python-protos $(BUILD)/ratelimit-go backend-client-image
	$(PYTHON) tests/backend_modes_docker.py --cpp $(BUILD)/ratelimit-cpp --go $(BUILD)/ratelimit-go --generated $(BUILD)/python
asan:
	$(MAKE) BUILD=$(ASAN_BUILD) CXXFLAGS='$(ASAN_CXXFLAGS)' LDFLAGS='$(ASAN_LDFLAGS)' all
test-asan: asan
	$(MAKE) BUILD=$(ASAN_BUILD) python-protos
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 $(PYTHON) tests/load.py --cpp $(ASAN_BUILD)/ratelimit-cpp --generated $(ASAN_BUILD)/python
tsan:
	$(MAKE) BUILD=$(TSAN_BUILD) CXXFLAGS='$(TSAN_CXXFLAGS)' LDFLAGS='$(TSAN_LDFLAGS)' all
test-tsan: tsan
	$(MAKE) BUILD=$(TSAN_BUILD) python-protos
	TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 $(PYTHON) tests/load.py --cpp $(TSAN_BUILD)/ratelimit-cpp --generated $(TSAN_BUILD)/python
test: test-unit test-smoke
run: all
	./$(BUILD)/ratelimit-cpp
install: all
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m755 $(BUILD)/ratelimit-cpp $(DESTDIR)$(PREFIX)/bin/ratelimit
docker-build:
	docker compose -f docker-compose.validation.yml build --no-cache
docker-test:
	bash docker/validate.sh
clean:
	$(PYTHON) -c 'import shutil; shutil.rmtree("$(BUILD)", ignore_errors=True)'
-include $(DEPS)
