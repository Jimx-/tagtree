#ifndef _TAGTREE_BITMAP_H_
#define _TAGTREE_BITMAP_H_

#include <cstddef>

namespace tagtree {

extern void bitmap_and(const void* a, const void* b, void* c, size_t size);
extern void bitmap_or(const void* a, const void* b, void* c, size_t size);

} // namespace tagtree

#endif
