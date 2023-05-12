# Finnish table - Extendible hashing hash table

## Features

 - fast insert
 - grows very fast
 - slower lookup
 - slower remove
 - meaningless name
 - bugs that will not be fixed. This is just an attempt to bully other people
   to do extendible hashing but better than I did. (Please don't just copy and
   pasta the ideas presented here or I will call you a fraud.)
 - it just works
 
## Tell me lies

It consists of a set of small hash tables and an array mapped trie. The trie
looks at the hash value of a key and tells which of the small hash tables that
key belongs to. This trie lookup is just an array indexing into the trie array.
This additional bit of structure opens up all kinds of opportunities.

This implementation stores 16 bits of the hash value for each entry, 8 bits for
"tophash" and 8 bits for "triehash". Triehash exists to speed up growing the
hash table. Both parts are used to find the matching key when probing so we
don't have to look at the actual key too much, tophash exists solely for that.

By storing the 8 bits of triehash, the growing mechanism of splitting can be
done without having to always recalculate hashes for the keys. When splitting,
half of the entries of the small hash table are moved to a new hash table and
the trie is updated to point to them both. The triehash tells us which of the
two hash tables each key belong to. Finnish table can double **multiple times**
in size before having to recover the full hash value. With this implementation
you get to store just above ~3k key-value pairs before your hash function is
needed to hash the keys for the second time. And from then on it's every 4th
split.

Growing involves data that is mostly already in the CPU caches because they are
loaded in the preceding linear probing, and there's only very little of it
(small hash tables are small). The amount of cache pollution that a single
growth-causing Put can do is pretty much limited to 2x the size of the small
hash table that became full.

Shrinks by halving the number of buckets in the small hash tables.

## Size hint / initial capacity

Unlike with the built-in Go map, the initial size hint doesn't end up being
rounded up to the next power-of-two number. By utilizing the fact that only the
trie needs to be of a power-of-two size but not the number of buckets per map,
Finnish table will adjust the number of buckets per map to allocate much more
precisely the minimal amount of space for the hinted amount of entries. Right
now you get a load factor of 0.75 after inserting the hinted amount of entries.

## Sweet little lies

Here Todd Howard sells you the same ~~game~~ data structure for the third time.

Look at the `BenchmarkLoadFactor` test to see how loaded the structure stays.
For the current implementation it gracefully glides up and down from 0.5 to 0.8
for various sizes. No big jumps there between two sizes.

`BenchmarkShortUse` simulates the absolute classic usage of hashmaps - you
create one, populate it, do some lookups and then throw it away! Here compared
against the built-in map of Go. So, create empty map (with no size hint),
insert _size_ unique entries, and then look up all those _size_ entries. Repeat
that _times_ times and there you have one "op." Key and value are both 64-bits.

    fish=false&size=7&times=10000        2199945 ns/op      1920000 B/op    20000 allocs/op
    fish=true&size=7&times=10000         3544430 ns/op      4000000 B/op    20000 allocs/op
    fish=false&size=33&times=1000        2846731 ns/op      2422751 B/op     9047 allocs/op
    fish=true&size=33&times=1000         2042681 ns/op      1344000 B/op     8000 allocs/op
    fish=false&size=121&times=1000      11314968 ns/op     11016912 B/op    20937 allocs/op
    fish=true&size=121&times=1000        7859773 ns/op      4676730 B/op    19508 allocs/op
    fish=false&size=795&times=1000      59302975 ns/op     45040234 B/op    56524 allocs/op
    fish=true&size=795&times=1000       50992625 ns/op     32820657 B/op    56517 allocs/op
    fish=false&size=1092&times=1000     90666254 ns/op     86812278 B/op    66993 allocs/op
    fish=true&size=1092&times=1000      77558622 ns/op     50044333 B/op    78711 allocs/op
    fish=false&size=1976&times=200      34487428 ns/op     34691945 B/op    20330 allocs/op
    fish=true&size=1976&times=200       27289408 ns/op     22544446 B/op    21221 allocs/op
    fish=false&size=2359&times=200      37926931 ns/op     34755320 B/op    20636 allocs/op
    fish=true&size=2359&times=200       30594804 ns/op     24784963 B/op    22193 allocs/op
    fish=false&size=6215&times=200      92299503 ns/op     71584121 B/op    43602 allocs/op
    fish=true&size=6215&times=200       77128146 ns/op     40706742 B/op    32196 allocs/op
    fish=false&size=500000&times=5     432175780 ns/op    218992418 B/op    96058 allocs/op
    fish=true&size=500000&times=5      353778978 ns/op     94694666 B/op    30912 allocs/op

fish=false is built-in map, fish=true is Finnish table. Please do note that my
preferred cpu is Intel/Haswell.

## Impact on the runtime

Extendible hashing hash table does not use one big memory allocation for the
backing storage, instead it consists of many (mostly) fixed size small
allocations. This may have some sort of impact on your
allocator/GC/whatever_runtime_you_have. Also, when growing by splitting, little
to no memory is thrown away, instead the small hash table that became full and
is now being "split" is re-used as the other half.

For example below is benchmark results showcasing the difference in the amount
of GC cycles and stop-the-world time between built-in Go map and Finnish table.
Here one _op_ is insert of _size_ key-value pairs into the hash table. We let
the GC run on multiple CPU cores as one does in real software, so it messes
things up a bit as it usually does.

    ext=false&size=263            55346 ns/op       290.0 gc-stw-ns/op    0.005799 gc/op
    ext=true&size=263             49792 ns/op       158.2 gc-stw-ns/op    0.002850 gc/op
    ext=false&size=723           179984 ns/op       810.7 gc-stw-ns/op    0.01176 gc/op
    ext=true&size=723            127750 ns/op       492.8 gc-stw-ns/op    0.008284 gc/op
    ext=false&size=1902          512626 ns/op      2852 gc-stw-ns/op      0.04754 gc/op
    ext=true&size=1902           331951 ns/op      1918 gc-stw-ns/op      0.03419 gc/op
    ext=false&size=6021         1535226 ns/op      7070 gc-stw-ns/op      0.09988 gc/op
    ext=true&size=6021          1216736 ns/op      3991 gc-stw-ns/op      0.05848 gc/op
    ext=false&size=10518        2488954 ns/op     12123 gc-stw-ns/op      0.1983 gc/op
    ext=true&size=10518         2292961 ns/op      6710 gc-stw-ns/op      0.09808 gc/op
    ext=false&size=39127        9071257 ns/op     53570 gc-stw-ns/op      0.9932 gc/op
    ext=true&size=39127         8977597 ns/op     24805 gc-stw-ns/op      0.3718 gc/op
    ext=false&size=124152      30414805 ns/op     53572 gc-stw-ns/op      1.024 gc/op
    ext=true&size=124152       29231045 ns/op     81096 gc-stw-ns/op      1.360 gc/op
    ext=false&size=2500000    381788105 ns/op    346223 gc-stw-ns/op      2.667 gc/op
    ext=true&size=2500000     245723356 ns/op    111994 gc-stw-ns/op      2.600 gc/op
 
The nice (but hard to benchmark) thing about having small(ish) allocations is
that when the hash table is freed then its backing memory is not one big chunk
of bytes that will never fit anyone else's needs. Smallish allocations have a
much larger likelihood[^1] of being reused by some other part of the program.
Especially if all parts of your program strive to allocate chunks of the same
size.

# Big sad

Plenty of cycles are being wasted in calling the functions written in asm.
Compiling with "--tags nosimd" is usually faster as it just does SWAR written
in Go. For any curious reader, the above benchmarks are all calling into the
asm.

[^1]: This was revealed to me in a dream. Beyond my cryptic dreams I'm pretty
  sure that there's a paper out there that repeats this message. One of course
  could just look at the allocator/GC code and think for oneself from that.
