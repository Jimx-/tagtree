#include "tagtree/index/bitmap.h"

#include <cstdint>

#ifdef _TAGTREE_USE_AVX2_
#include <immintrin.h>
#endif

namespace tagtree {

#ifdef _TAGTREE_USE_AVX2_

void bitmap_and(const void* a, const void* b, void* c, size_t size)
{
    const uint8_t* pa = (const uint8_t*)a;
    const uint8_t* pb = (const uint8_t*)b;
    uint8_t* pc = (uint8_t*)c;
    const size_t block_width = 32;

    while (pa < ((uint8_t*)a + size)) {
        __m256i ma = _mm256_loadu_si256((const __m256i*)pa);
        __m256i mb = _mm256_loadu_si256((const __m256i*)pb);
        __m256i mc = _mm256_and_si256(ma, mb);
        _mm256_storeu_si256((__m256i*)pc, mc);

        pa += block_width;
        pb += block_width;
        pc += block_width;
    }
}

void bitmap_or(const void* a, const void* b, void* c, size_t size)
{
    const uint8_t* pa = (const uint8_t*)a;
    const uint8_t* pb = (const uint8_t*)b;
    uint8_t* pc = (uint8_t*)c;
    const size_t block_width = 32;

    while (pa < ((uint8_t*)a + size)) {
        __m256i ma = _mm256_loadu_si256((const __m256i*)pa);
        __m256i mb = _mm256_loadu_si256((const __m256i*)pb);
        __m256i mc = _mm256_or_si256(ma, mb);
        _mm256_storeu_si256((__m256i*)pc, mc);

        pa += block_width;
        pb += block_width;
        pc += block_width;
    }
}

#else
void bitmap_and(const void* a, const void* b, void* c, size_t size)
{
    const uint64_t* pa = (const uint64_t*)a;
    const uint64_t* pb = (const uint64_t*)b;
    uint64_t* pc = (uint64_t*)c;

    while (pa < (uint64_t*)((uint8_t*)a + size)) {
        *pc = *pa & *pb;

        pa++;
        pb++;
        pc++;
    }
}

void bitmap_or(const void* a, const void* b, void* c, size_t size)
{
    const uint64_t* pa = (const uint64_t*)a;
    const uint64_t* pb = (const uint64_t*)b;
    uint64_t* pc = (uint64_t*)c;

    while (pa < (uint64_t*)((uint8_t*)a + size)) {
        *pc = *pa | *pb;

        pa++;
        pb++;
        pc++;
    }
}

#endif

} // namespace tagtree
