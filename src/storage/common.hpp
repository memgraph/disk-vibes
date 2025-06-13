#pragma once

#include <iostream>

#include <spdlog/spdlog.h>

void __mg_assert(const char *expr, const char *comment, const char *file, int line) {
  std::cerr << "Assertion failed: " << expr << ", " << comment << " in " << file << "(" << line << ")" << std::endl;
  std::abort();
}

#define MG_ASSERT(expr, comment)                                                                                       \
  if (!(expr)) {                                                                                                       \
    __mg_assert(#expr, comment, __FILE__, __LINE__);                                                                   \
  }

void init_spdlog() {
  // [time] [thread_id] [log_level] message
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%^%l%$] %v");
  spdlog::set_level(spdlog::level::info);
}