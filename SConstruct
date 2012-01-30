env = Environment(CCFLAGS = '-std=c99')
bloom = env.Library('bloom', Glob("src/*.c"))
env.Program('test_runner', bloom + Glob("tests/*.c"), LIBS=["libcheck"])
