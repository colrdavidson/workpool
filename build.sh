OS=$(uname)

OS_FLAGS=""
case $OS in
Darwin)
	OS_FLAGS="-framework System"
	;;
esac

clang -g -O3 -Wall -o pool $OS_FLAGS -ldl -lpthread -finstrument-functions main.c
