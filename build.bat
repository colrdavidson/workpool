@echo off

clang -fuse-ld=lld -O3 -g -o pool.exe -D_CRT_SECURE_NO_WARNINGS -finstrument-functions main.c

rem cl main.c /O2 /Zi /GH /Gh /diagnostics:caret /nologo /Fe:pool.exe
