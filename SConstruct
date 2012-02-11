envspooky = Environment(CPPPATH = ['deps/spookyhash/'], CPPFLAGS="-O2")
spooky = envspooky.Library('spooky', Glob("deps/spookyhash/*.cpp"))

envmurmur = Environment(CPPPATH = ['deps/murmurhash/'], CPPFLAGS="-O2")
murmur = envmurmur.Library('murmur', Glob("deps/murmurhash/*.cpp"))

envmurmur = Environment(CPATH = ['deps/inih/'], CFLAGS="-O2")
inih = envmurmur.Library('inih', Glob("deps/inih/*.c"))

envbloom = Environment(CCFLAGS = '-std=c99 -Wall -Werror -O2')
bloom = envbloom.Library('bloom', Glob("src/libbloom/*.c"))

envtest = Environment(CCFLAGS = '-std=c99')
envtest.Program('test_runner', spooky + murmur + bloom + Glob("tests/*.c"), LIBS=["libcheck"])

