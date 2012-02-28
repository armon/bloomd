envspooky = Environment(CPPPATH = ['deps/spookyhash/'], CPPFLAGS="-O2")
spooky = envspooky.Library('spooky', Glob("deps/spookyhash/*.cpp"))

envmurmur = Environment(CPPPATH = ['deps/murmurhash/'], CPPFLAGS="-O2")
murmur = envmurmur.Library('murmur', Glob("deps/murmurhash/*.cpp"))

envbloom = Environment(CCFLAGS = '-std=c99 -Wall -Werror -O2')
bloom = envbloom.Library('bloom', Glob("src/libbloom/*.c"))

envtest = Environment(CCFLAGS = '-std=c99 -Isrc/libbloom/')
envtest.Program('test_libbloom_runner', spooky + murmur + bloom +  Glob("tests/libbloom/*.c"), LIBS=["libcheck"])

envinih = Environment(CPATH = ['deps/inih/'], CFLAGS="-O2")
inih = envinih.Library('inih', Glob("deps/inih/*.c"))

envbloomd_with_err = Environment(CCFLAGS = '-std=c99 -g -Wall -Werror -O2 -pthread -Ideps/inih/ -Ideps/libev/ -Isrc/libbloom/')
envbloomd_without_err = Environment(CCFLAGS = '-std=c99 -g -O2 -pthread -Isrc/bloomd/ -Ideps/inih/ -Ideps/libev/ -Isrc/libbloom/')

objs =  envbloomd_with_err.Object('src/bloomd/config', 'src/bloomd/config.c') + \
        envbloomd_without_err.Object('src/bloomd/networking', 'src/bloomd/networking.c') + \
        envbloomd_with_err.Object('src/bloomd/conn_handler', 'src/bloomd/conn_handler.c') + \
        envbloomd_with_err.Object('src/bloomd/hashmap', 'src/bloomd/hashmap.c') + \
        envbloomd_with_err.Object('src/bloomd/filter', 'src/bloomd/filter.c') + \
        envbloomd_with_err.Object('src/bloomd/filter_manager', 'src/bloomd/filter_manager.c')

envbloomd_with_err.Program('bloomd', spooky + murmur + bloom + inih + objs + ["src/bloomd/bloomd.c"])
envbloomd_without_err.Program('test_bloomd_runner', spooky + murmur + bloom + inih + objs + Glob("tests/bloomd/runner.c"), LIBS=["libcheck"])

