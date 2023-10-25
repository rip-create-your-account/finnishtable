# Finnish table - Extendible hashing hash table - the minimal and perfect variant

## Features

 - Mega fast construction
 - Giga fast lookup
 - Omega fast lookup for keys not in the map - they are rejected out early
   before having to compare keys
 - Uses a few bytes of metadata per entry
 - Plenty of little things left over to optimize. I shat this out in ~6 hours
   without thinking about this beforehand. I just wanted to see how easy it
   would be to adapt extendible hashing into building a perfect minimal hash
   table. Answer: it's easy and the end result is very practical.
 - I shat this out quickly so it might have some cases where performance goes
   kaputt. Use a good hash function.
 
## Tell me lies

Benchmarks use a 64-bit key and a 64-bit value. Please do note that my
preferred cpu is Intel/Haswell and over 10yrs old. Also Go is dogshit and
generates shit code because it has to. Assume that in a language which has a
smaller runtime and a better compiler you can get significantly better
performance. For example, note the fact that `Construct/size=1023` took only
__15334ns__ for the fastest run while the "average" across multiple runs is
__20977ns__. Garbage collection/the runtime really messes these numbers up.
It's not the algorithm randomly having bad luck, trust me bro.
 
### Lookup

    Lookup/size=1                   11.72 ns/op    11.72 ns/elem
    Lookup/size=7                   80.00 ns/op    11.43 ns/elem
    Lookup/size=13                 148.1 ns/op     11.40 ns/elem
    Lookup/size=29                 336.4 ns/op     11.60 ns/elem
    Lookup/size=63                 727.7 ns/op     11.55 ns/elem
    Lookup/size=121               1399 ns/op       11.56 ns/elem
    Lookup/size=263               3021 ns/op       11.49 ns/elem
    Lookup/size=723               8329 ns/op       11.52 ns/elem
    Lookup/size=1023             11769 ns/op       11.50 ns/elem
    Lookup/size=1902             21985 ns/op       11.56 ns/elem
    Lookup/size=3298             38500 ns/op       11.67 ns/elem
    Lookup/size=6021             72530 ns/op       12.05 ns/elem
    Lookup/size=10518           131604 ns/op       12.51 ns/elem
    Lookup/size=39127           586065 ns/op       14.98 ns/elem
    Lookup/size=76124          1370590 ns/op       18.00 ns/elem
    Lookup/size=124152         2336336 ns/op       18.82 ns/elem
    Lookup/size=1000001       40847811 ns/op       40.85 ns/elem
    Lookup/size=2500000      148296162 ns/op       59.32 ns/elem
    Lookup/size=9648634      897224488 ns/op       92.99 ns/elem
    Lookup/size=100000000   9315526581 ns/op       93.16 ns/elem
    
Perfect as it should be. Cache+TLB misses be damned.

### Construct

    Construct/size=29                   888.2 ns/op    42.21 bits/elem     30.63 ns/elem            553.0 ns/fastest
    Construct/size=63                  2103 ns/op      26.03 bits/elem     33.39 ns/elem            989.0 ns/fastest
    Construct/size=121                 2933 ns/op      22.08 bits/elem     24.24 ns/elem           1844 ns/fastest
    Construct/size=263                 5564 ns/op      17.86 bits/elem     21.16 ns/elem           3700 ns/fastest
    Construct/size=723                16047 ns/op      17.75 bits/elem     22.20 ns/elem          11506 ns/fastest
    Construct/size=1023               20977 ns/op      19.75 bits/elem     20.51 ns/elem          15334 ns/fastest
    Construct/size=1902               42180 ns/op      15.55 bits/elem     22.18 ns/elem          31907 ns/fastest
    Construct/size=3298               76183 ns/op      16.03 bits/elem     23.10 ns/elem          59977 ns/fastest
    Construct/size=6021              149630 ns/op      16.45 bits/elem     24.85 ns/elem         120693 ns/fastest
    Construct/size=10518             231481 ns/op      17.24 bits/elem     22.01 ns/elem         190247 ns/fastest
    Construct/size=39127            1084451 ns/op      17.65 bits/elem     27.72 ns/elem         933528 ns/fastest
    Construct/size=76124            2471467 ns/op      24.73 bits/elem     32.47 ns/elem        1971245 ns/fastest
    Construct/size=124152           5086350 ns/op      19.31 bits/elem     40.97 ns/elem        4382498 ns/fastest
    Construct/size=1000001        113647262 ns/op      19.25 bits/elem    113.6 ns/elem       104782296 ns/fastest
    Construct/size=2500000        345090798 ns/op      24.36 bits/elem    138.0 ns/elem       306863474 ns/fastest
    Construct/size=9648634       1360878778 ns/op      24.84 bits/elem    141.0 ns/elem      1360878125 ns/fastest
    Construct/size=100000000    19505955878 ns/op      21.60 bits/elem    195.1 ns/elem     19505954720 ns/fastest

So this thing constructs faster than most normal hash tables do. And there's
some optimizations to do. If you did all the optimizations and tuned this for
construction+lookup performance then I would assume construction times of
~10ns/elem. See below and all the TODOs littered around the code.

Memory overhead is ~20 bits per key. Totally worth it.

# Big sad

Plenty of cycles are being wasted in calling the functions written in asm.

# Silly stuff

I'm pretty sure that locality-sensitive hashing would work just as well here as
it does for the normal Finnish table.