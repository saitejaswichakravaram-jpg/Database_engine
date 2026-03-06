CXX      := clang++
CXXFLAGS := -std=c++17 -Wall -Wextra -g -O1 -Iinclude
BUILDDIR := build

SRCS := \
	src/storage/disk_manager.cpp \
	src/storage/table_page.cpp \
	src/storage/table_heap.cpp \
	src/buffer/lru_replacer.cpp \
	src/buffer/buffer_pool_manager.cpp \
	src/index/bplus_tree.cpp \
	src/concurrency/lock_manager.cpp \
	src/concurrency/transaction.cpp \
	src/recovery/log_manager.cpp \
	src/recovery/recovery_manager.cpp \
	src/main.cpp

OBJS := $(patsubst src/%.cpp,$(BUILDDIR)/%.o,$(SRCS))

TARGET := $(BUILDDIR)/ydb

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BUILDDIR)/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILDDIR)
