OS=$(uname)

OS_FLAGS=""
case $OS in
Darwin)
	OS_FLAGS="-framework System"
	;;
esac

OPT_FLAGS=""
case $1 in
	-t)
	OPT_FLAGS+="-finstrument-functions -D ENABLE_TRACING"
	shift
	;;
esac

clang -g -O3 -Wall -o pool $OS_FLAGS -ldl -lpthread $OPT_FLAGS main.c
