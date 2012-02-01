envspooky = Environment(CPPPATH = ['deps/spookyhash/'])
spooky = envspooky.Library('spooky', Glob("deps/spookyhash/*.cpp"))

envmurmur = Environment(CPPPATH = ['deps/murmurhash/'])
murmur = envmurmur.Library('murmur', Glob("deps/murmurhash/*.cpp"))

envbloom = Environment(CCFLAGS = '-std=c99 -Wall -Werror')
bloom = envbloom.Library('bloom', Glob("src/*.c"))

envtest = Environment(CCFLAGS = '-std=c99')
envtest.Program('test_runner', spooky + murmur + bloom + Glob("tests/*.c"), LIBS=["libcheck"])

