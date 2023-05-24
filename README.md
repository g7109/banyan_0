Brane
=======
Building Brane
--------------------

First, clone actor-codegen-tool submodule.
```
> git submodule update --init
```
Assuming that you would like to use system packages (RPMs or DEBs) for Seastar's dependencies, first install them:
```
> sudo ./install-dependencies.sh
```

then, set your libclang path as libclang_path enviroment variable

```
> libclang_path = ${Your libclang path}
```

then, configure and install.

(A) build seastar-actor without installation.

```
# Configure
# Open brane gpu resource pool by add option "--open-gpu-resource-pool" when configuring.
> ./configure.py --mode=release --cook fmt --compiler=g++-8 \
        --c++-dialect="gnu++17" --libclang-path=${libclang_path}
# Build
>  ninja -C build/release
```

(B) build an installed seastar-actor

```
>  ./configure.py --mode=release --cook fmt --compiler=g++-8 \ 
        --c++-dialect="gnu++17" --prefix=/usr/local \ 
        --libclang-path=${libclang_path}
>  ninja -C build/release install 
```

