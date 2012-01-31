env = Environment(CCFLAGS = '-std=c99 -Wall -Werror')
bloom = env.Library('bloom', Glob("src/*.c"))

env_ignore = Environment(CCFLAGS = '-std=c99')
env_ignore.Program('test_runner', bloom + Glob("tests/*.c"), LIBS=["libcheck"])
