# Finnish table - Extendible hashing hash table - the perfect variant

## Features

 - Mega fast construction
 - Giga fast lookup
 - Omega fast lookup for keys not in the map - they are rejected out early
   before having to compare keys
 - Uses a few bytes of metadata per entry
 - Plenty of little things left over to optimize. I shat this out in ~6 hours
   without thinking about this beforehand. I just wanted to see how easy it
   would be to adapt extendible hashing into building a perfect hash table.
   Answer: it's easy and the end result is very practical.
 - I shat this out quickly so it might have some cases where performance goes
   kaputt. Use a good hash function.
 
## Tell me lies

Benchmarks use a 64-bit key and a 64-bit value. Please do note that my
preferred cpu is Intel/Haswell and over 10yrs old. Also Go is dogshit and
generates shit code because it has to. I'm pretty sure that all the numbers
here are at least 2x of what they should be for my cpu.
 
### Lookup
 
    Lookup/size=1                       11.84 ns/op     11.84 ns/elem
    Lookup/size=7                       80.61 ns/op     11.52 ns/elem
    Lookup/size=13                     153.7 ns/op      11.83 ns/elem
    Lookup/size=29                     343.9 ns/op      11.86 ns/elem
    Lookup/size=63                     728.8 ns/op      11.57 ns/elem
    Lookup/size=121                   1415 ns/op        11.69 ns/elem
    Lookup/size=263                   3043 ns/op        11.57 ns/elem
    Lookup/size=723                   8359 ns/op        11.56 ns/elem
    Lookup/size=1023                 11828 ns/op        11.56 ns/elem
    Lookup/size=1902                 22087 ns/op        11.61 ns/elem
    Lookup/size=3298                 38444 ns/op        11.66 ns/elem
    Lookup/size=6021                 71414 ns/op        11.86 ns/elem
    Lookup/size=10518               131123 ns/op        12.47 ns/elem
    Lookup/size=39127               610635 ns/op        15.61 ns/elem
    Lookup/size=76124              1243934 ns/op        16.34 ns/elem
    Lookup/size=124152             2179982 ns/op        17.56 ns/elem
    Lookup/size=1000001           40730059 ns/op        40.73 ns/elem
    Lookup/size=2500000          220657877 ns/op        88.26 ns/elem
    Lookup/size=9648634         1044024482 ns/op        108.2 ns/elem
    Lookup/size=100000000      11728421068 ns/op        117.3 ns/elem
    
Perfect as it should be. Cache+TLB misses be damned.

### Construct

    Construct/size=1                    240.9 ns/op     240.9 ns/elem    0.1250 xbytes-factor
    Construct/size=7                    350.9 ns/op     50.13 ns/elem    0.5000 xbytes-factor
    Construct/size=13                   474.9 ns/op     36.53 ns/elem    0.6500 xbytes-factor
    Construct/size=29                  1041 ns/op       35.90 ns/elem    0.7733 xbytes-factor
    Construct/size=63                  2191 ns/op       34.78 ns/elem    0.8077 xbytes-factor
    Construct/size=121                 3893 ns/op       32.17 ns/elem    0.8388 xbytes-factor
    Construct/size=263                 7264 ns/op       27.62 ns/elem    0.8362 xbytes-factor
    Construct/size=723                18306 ns/op       25.32 ns/elem    0.8602 xbytes-factor
    Construct/size=1023               30840 ns/op       30.15 ns/elem    0.8731 xbytes-factor
    Construct/size=1902               54921 ns/op       28.88 ns/elem    0.8724 xbytes-factor
    Construct/size=3298               89428 ns/op       27.12 ns/elem    0.8687 xbytes-factor
    Construct/size=6021              163358 ns/op       27.13 ns/elem    0.8646 xbytes-factor
    Construct/size=10518             301333 ns/op       28.65 ns/elem    0.8602 xbytes-factor
    Construct/size=39127            1363398 ns/op       34.85 ns/elem    0.8568 xbytes-factor
    Construct/size=76124            2957578 ns/op       38.85 ns/elem    0.8557 xbytes-factor
    Construct/size=124152           5986055 ns/op       48.22 ns/elem    0.8492 xbytes-factor
    Construct/size=1000001        105817824 ns/op       105.8 ns/elem    0.8493 xbytes-factor
    Construct/size=2500000        300606082 ns/op       120.2 ns/elem    0.8200 xbytes-factor
    Construct/size=9648634       1447385812 ns/op       150.0 ns/elem    0.8175 xbytes-factor
    Construct/size=100000000    16093236767 ns/op       160.9 ns/elem    0.8363 xbytes-factor
    
So this thing constructs faster than most normal hash tables do. And there's
plenty of optimizations to do. See below and all the TODOs littered around the
code.

# Big sad

Plenty of cycles are being wasted in calling the functions written in asm.

# Silly stuff

I'm pretty sure that locality-sensitive hashing would work just as well here as
it does for the normal Finnish table.