#ifndef __SPYKFUNC_HADOKEN_RANDOM
#define __SPYKFUNC_HADOKEN_RANDOM

#include <limits>

#include "hadoken/random/random.hpp"

typedef hadoken::random_engine_mapper_64 rng;

constexpr uint64_t MAXVAL = std::numeric_limits<uint64_t>::max();

#endif
